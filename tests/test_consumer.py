"""A durable client: one that keeps its place in a channel.

What saguin promises, and what these measure against.  A client
connecting with clean start off and a session expiry that is not zero
keeps a position, stored per client id per channel, and resumes there -
receiving everything published while it was away, in order.  A client
with no stored position is served the channel from its retention floor;
that replay is what the channel type is for.  The position is the lowest
offset not acknowledged, so it never steps over a record the client did
not finish.  A client may ask for any session expiry and the broker caps
it, saying so in the CONNACK.  And a dead-letter channel is an `append`
channel in every other respect, read like any other, its records carrying
the broker's own account of why the work failed.
"""

import os
import ssl
import time
from datetime import datetime, timezone

import pytest

import saguin


def payloads(records, count):
    """The next `count` records, acknowledged as they are taken - which is
    what the iterator does when the loop asks for the next one, so reading
    one more than wanted is what acknowledges the last."""
    got = []
    for record in records:
        got.append(record.payload)
        if len(got) == count:
            break
    return got


def test_a_client_with_no_position_is_served_the_channel_from_the_start(
    producer, broker, site
):
    for n in (b"1", b"2", b"3"):
        producer.client.append.publish("events", key=[site, "thing"], value=n)

    with saguin.Client("reader-" + site, durable=True) as c:
        c.start(*broker.address)
        assert c.session_present is False
        assert payloads(c.append.consume("events", key=[site], timeout=15), 3) == [
            b"1", b"2", b"3",
        ]


def test_a_client_resumes_where_it_stopped(producer, broker, site):
    """And the record whose turn of the loop did not finish comes back,
    because the position is the lowest offset not acknowledged."""
    name = "resumer-" + site
    for n in (b"1", b"2"):
        producer.client.append.publish("events", key=[site, "thing"], value=n)

    with saguin.Client(name, durable=True) as first:
        first.start(*broker.address)
        # Two taken, one acknowledged: leaving the loop holding b"2" leaves
        # it unacknowledged, which is the case worth driving.
        assert payloads(first.append.consume("events", key=[site], timeout=15), 2) == [
            b"1", b"2",
        ]

    producer.client.append.publish("events", key=[site, "thing"], value=b"3")

    with saguin.Client(name, durable=True) as again:
        again.start(*broker.address)
        assert again.session_present is True
        assert payloads(again.append.consume("events", key=[site], timeout=15), 2) == [
            b"2", b"3",
        ]


def test_the_broker_says_what_session_expiry_it_granted(broker, site):
    """The configured cap is five minutes, so one of these is answered with
    what it asked for and the other with the cap."""
    with saguin.Client("under-" + site, durable=True, session_expiry=120) as under:
        under.start(*broker.address)
        assert under.granted_session_expiry == 120

    with saguin.Client("over-" + site, durable=True, session_expiry=86400) as over:
        over.start(*broker.address)
        assert over.granted_session_expiry == 300
        assert over.session_expiry == 86400


def test_a_client_that_keeps_no_place_is_not_durable(broker, site):
    """A plain client is a publisher: it may write, and it stores nothing."""
    with saguin.Client("plain-" + site) as c:
        c.start(*broker.address)
        assert c.durable is False
        assert c.session_expiry == 0
        c.append.publish("events", key=[site, "thing"], value=b"x").wait_for_publish(10)


def test_a_refused_filter_is_raised_rather_than_left_silent(broker, site):
    """A queue's own filter is not an ordinary subscription, and the broker
    answers a reason code per filter - which a client that does not read
    them experiences as being connected and receiving nothing for ever.

    Driven through the guard the verbs use, because with no `acl_file`
    configured this broker refuses nothing a verb would ask for: the verbs
    subscribe through a queue's pin rather than its filter, which is
    exactly why they are not the way to reach this.
    """
    with saguin.Client("refused-" + site, durable=True) as c:
        c.start(*broker.address)
        with pytest.raises(saguin.SubscriptionRefused) as refused:
            c._subscribe_and_check(["work/+/jobs/+"], 1, 10)

        assert refused.value.refused[0][0] == "work/+/jobs/+"
        assert refused.value.refused[0][1].is_failure

        # A granted filter beside a refused one is named, so a client can
        # tell which of several it asked for was turned back.
        with pytest.raises(saguin.SubscriptionRefused) as mixed:
            c._subscribe_and_check(
                ["work/+/jobs/+", "iot/{}/events/+".format(site)], 1, 10
            )
        assert [f for f, _ in mixed.value.refused] == ["work/+/jobs/+"]
        assert len(mixed.value.reason_codes) == 2
        assert mixed.value.reason_codes[1].is_failure is False


def test_a_dead_letter_says_which_queue_it_came_from_and_why(
    producer, reader, broker, site
):
    """The queue is configured for one attempt, so a worker that hands the
    job back has exhausted it."""
    job = producer.client.queue.publish("tasks", key=[site, "1"], value=b"do it")
    job.wait_for_publish(10)

    with reader() as worker:
        worker.subscribe("$saguin/queue/tasks", qos=1)
        worker.answer(worker.next(), "return")

    with saguin.Client("dlq-" + site, durable=True) as c:
        c.start(*broker.address)
        dead = next(c.append.consume("tasks__dlq", key=[site], timeout=30))

    assert dead.payload == b"do it"
    assert dead.id == job.saguin_id, "a dead letter keeps the record's own id"
    assert dead.dlq is not None
    assert dead.dlq.channel == "tasks"
    assert dead.dlq.reason == "attempts_exhausted"
    assert dead.dlq.attempts == 1
    assert isinstance(dead.dlq.offset, int)
    assert dead.dlq.at.tzinfo is not None
    assert abs((datetime.now(timezone.utc) - dead.dlq.at).total_seconds()) < 300
    assert dead.dlq.first is not None and dead.dlq.last is not None

    # The broker's account of the failure is not left among the publisher's
    # own headers, where a consumer would read it as one.
    assert "saguin-dlq-reason" not in dead.headers


def test_an_ordinary_record_has_no_dead_letter_account(producer, reader, site):
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        producer.client.append.publish("events", key=[site, "thing"], value=b"x")
        assert saguin.Message(r.next()).dlq is None


# -- what the class refuses, with no broker in it ---------------------------


def test_a_client_needs_an_id_of_its_own():
    with pytest.raises(ValueError) as refused:
        saguin.Client("")
    assert "id of its own" in str(refused.value)


def test_a_durable_client_refuses_a_session_that_ends_with_the_connection():
    with pytest.raises(ValueError):
        saguin.Client("someone", durable=True, session_expiry=0)


def test_a_durable_client_refuses_a_clean_start(broker):
    c = saguin.Client("clean-start-asker", durable=True)
    with pytest.raises(ValueError) as refused:
        c.connect(broker.address[0], broker.address[1], clean_start=True)
    assert "clean start off" in str(refused.value)


def test_a_connection_the_broker_refuses_is_raised(broker):
    """**The one error path on the way in**, and nothing reached it until
    the harness grew a door that wants a password: every other test
    connects to a listener that admits anybody, so a refused CONNECT had
    never been driven. An error class nothing has ever raised is an error
    class nobody knows works.
    """
    host, port = broker.guarded
    with pytest.raises(saguin.ConnectRefused) as refused:
        saguin.Client("nameless", transport="websockets").start(host, port)
    assert refused.value.reason_code.is_failure
    assert "refused the connection" in str(refused.value)


def test_the_same_door_admits_a_client_with_the_password(broker):
    """The control: without it the test above passes against a listener
    that is simply broken."""
    host, port = broker.guarded
    c = saguin.Client("named", transport="websockets")
    c.username_pw_set("operator", "hunter2")
    try:
        c.start(host, port)
        assert c.connect_reason_code.value == 0
    finally:
        c.close()


# -- the door behind TLS ----------------------------------------------------
#
# **Every other test in this suite connects in the clear.** The library
# leaves paho's connecting methods exactly as they are, so `tls_set()` is
# the whole of the API and there is nothing of saguin's own to get wrong -
# which is also why nothing proved a saguin client reaches a saguin broker
# over TLS at all. These drive it, and each refusal has its own case
# beside its happy one: a library that connects when it should refuse is
# the failure that looks like success.


def certificate(tls_broker, name):
    return os.path.join(tls_broker.workdir, name)


def test_a_client_that_trusts_the_broker_connects_over_tls(tls_broker):
    """And publishes, because a handshake that completes and a connection
    that works are not the same claim."""
    c = saguin.Client("trusts-tls")
    c.tls_set(ca_certs=certificate(tls_broker, "ca.pem"))
    try:
        c.start(*tls_broker.address, timeout=15)
        assert c.connect_reason_code.value == 0
        c.append.publish(
            "events", key=["tls-site", "a"], value=b"over tls"
        ).wait_for_publish(10)
    finally:
        c.close()


def test_a_client_that_does_not_trust_the_broker_is_refused(tls_broker):
    """The control, and the half that matters: without it the test above
    passes against a client that would connect to anybody."""
    c = saguin.Client("stranger-tls")
    c.tls_set()          # the system's own roots, which signed nothing here
    with pytest.raises(ssl.SSLCertVerificationError) as refused:
        c.start(*tls_broker.address, timeout=15)
    assert "certificate verify failed" in str(refused.value)


def test_a_client_presenting_a_certificate_is_admitted(tls_broker):
    """The WebSocket listener names a `client_ca_file`, so it asks every
    client for a certificate and checks it against that authority."""
    c = saguin.Client("device-7", transport="websockets")
    c.tls_set(ca_certs=certificate(tls_broker, "ca.pem"),
              certfile=certificate(tls_broker, "device-7.pem"),
              keyfile=certificate(tls_broker, "device-7-key.pem"))
    try:
        c.start(tls_broker.guarded[0], tls_broker.guarded[1], timeout=15)
        assert c.connect_reason_code.value == 0
    finally:
        c.close()


def test_a_client_presenting_no_certificate_is_refused_where_one_is_required(
    tls_broker,
):
    """Trusting the broker is not enough at this door: the checking goes
    both ways, and a client with nothing to show is dropped in the
    handshake rather than at the CONNECT."""
    c = saguin.Client("nobody-tls", transport="websockets")
    c.tls_set(ca_certs=certificate(tls_broker, "ca.pem"))
    with pytest.raises(ssl.SSLError) as refused:
        c.start(tls_broker.guarded[0], tls_broker.guarded[1], timeout=15)
    assert "CERTIFICATE_REQUIRED" in str(refused.value), str(refused.value)


def test_a_durable_reader_survives_the_link_dropping(producer, broker, site):
    """**The state transition nothing drove.** Every other test connects,
    works and disconnects cleanly - so what a reader does when the socket
    dies under it, and what paho's automatic reconnect does to the
    per-filter routing, was unproven.

    The link is broken with saguin's own verb rather than by reaching into
    paho: another client hangs this one up, which is the real case an
    operator causes.
    """
    import time

    name = "survivor-" + site
    with saguin.Client(name, durable=True) as c:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], timeout=20)

        producer.client.append.publish("events", key=[site, "before"], value=b"before")
        assert next(records).payload == b"before"

        # Down it goes.
        assert producer.client.admin.disconnect(name) == "hung-up"
        for _ in range(100):
            if not c.is_connected():
                break
            time.sleep(0.05)
        assert not c.is_connected(), "the client never noticed the link go"

        # paho brings it back; the session is durable, so the broker
        # restores the subscription.
        for _ in range(200):
            if c.is_connected():
                break
            time.sleep(0.05)
        assert c.is_connected(), "paho did not reconnect within 10s"
        assert c.session_present is True, "the session was not resumed"

        producer.client.append.publish("events", key=[site, "after"], value=b"after")

        # **`before` comes back first, and that is the promise rather than
        # a surprise.** The iterator acknowledges a record when the loop
        # asks for the next one, so the one taken above was still
        # unacknowledged when the link went - and at-least-once means it
        # is sent again. Expecting `after` here was expecting the guarantee
        # to be broken.
        got = []
        for record in records:
            got.append(record.payload)
            if b"after" in got:
                break
        assert got == [b"before", b"after"], (
            "the reader went silent after the link came back, or lost the "
            "record it had not acknowledged: {}".format(got)
        )


def test_a_client_that_keeps_no_place_loses_its_subscription_with_the_link(
    producer, broker, site
):
    """**And the case that does not survive, written down rather than
    met.** A client with no durable session has nothing for the broker to
    restore, and paho does not subscribe again by itself - so a reader on
    one goes quiet when the link drops and nothing says so.

    That is MQTT's own behaviour rather than this library's, and the
    remedy is `durable=True`. It is a test so that it cannot change
    without somebody noticing.
    """
    import time

    name = "fragile-" + site
    with saguin.Client(name) as c:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], timeout=6)

        producer.client.append.publish("events", key=[site, "before"], value=b"before")
        assert next(records).payload == b"before"

        assert producer.client.admin.disconnect(name) == "hung-up"
        for _ in range(200):
            if not c.is_connected():
                break
            time.sleep(0.05)
        for _ in range(200):
            if c.is_connected():
                break
            time.sleep(0.05)
        assert c.is_connected(), "paho did not reconnect within 10s"
        assert c.session_present is False, "a client with no place kept one"

        producer.client.append.publish("events", key=[site, "after"], value=b"after")
        assert [r.payload for r in records] == [], (
            "the subscription came back on a client that keeps no place"
        )


def test_a_partly_refused_subscribe_leaves_nothing_subscribed(
    producer, broker, site
):
    """**The granted half of a partly refused SUBSCRIBE is undone.**

    MQTT answers each filter separately, so a SUBSCRIBE naming two
    filters can be half granted - and the caller gets an exception rather
    than a reader. Left alone, the granted filter goes on delivering to a
    client with nobody reading it: the records pile into the unread
    buffer, and on a queue they would be leases handed to a worker that
    does not exist.

    Driven at the guard the verbs use, because that is where a filter
    list is sent. Through the verbs it needs an `acl_file` granting one
    spelling of a braced channel and refusing the other, which was
    checked separately against a broker configured that way.
    """
    import queue as q

    seen = q.Queue()
    with saguin.Client("partly-" + site, durable=False) as c:
        c.start(*broker.address)
        c.on_message = lambda cl, u, msg: seen.put(msg)

        events = "iot/{}/events/+".format(site)
        with pytest.raises(saguin.SubscriptionRefused):
            c._subscribe_and_check(["work/+/jobs/+", events], 1, 10)

        # Nothing arrives on the filter the broker granted.
        producer.client.append.publish("events", key=[site, "one"], value=b"1")
        with pytest.raises(q.Empty):
            seen.get(timeout=3)

        # And the record was there all along: this client had been
        # unsubscribed from it rather than the broker having gone quiet.
        c.subscribe(events, qos=1)
        producer.client.append.publish("events", key=[site, "two"], value=b"2")
        assert seen.get(timeout=10).payload in (b"1", b"2")


def test_a_queue_job_left_in_hand_is_said_out_loud(producer, broker, site, caplog):
    """**A job taken and abandoned does not come back until this client
    disconnects**, and nothing else anywhere would say so.

    Its lease starts at the acknowledgement, so a job that was never
    acknowledged has no lease to expire: the broker holds it for a worker
    that has stopped asking, and the queue's depth shows a job nothing is
    working with no reason on the face of it. Not lost and not
    duplicated - which is exactly why it needs saying.
    """
    producer.client.queue.publish("retried", key=[site, "j1"], value=b"one")

    with caplog.at_level("WARNING", logger="saguin"):
        with saguin.Client("abandoner-" + site, durable=False) as w:
            w.start(*broker.address)
            jobs = w.queue.fetch("retried", timeout=10)
            assert next(jobs).payload == b"one"
            jobs.close()

    said = "\n".join(r.getMessage() for r in caplog.records)
    for want in ("retried", "neither acknowledged nor handed back", "queue.nack"):
        assert want in said, "the warning does not say {!r}: {}".format(want, said)

    # **The queue is left as it was found**, which is this test's own
    # mess to clear up: the job comes back when the client above
    # disconnects, and a queue is not narrowed by a site key the way a
    # channel is - every worker of a queue is offered every job in it. A
    # stray left here is handed to whichever test reads this queue next,
    # which is exactly how it failed once: a schema test asserted on the
    # first job it was offered and got this one.
    with saguin.Client("drainer-" + site, durable=False) as w:
        w.start(*broker.address)
        for job in w.queue.fetch("retried", timeout=10):
            w.queue.ack(job)
            break


def test_an_append_reader_left_in_hand_says_nothing(producer, broker, site, caplog):
    """The same shape on an append channel is ordinary and must stay
    quiet: the position has not advanced, so the record comes back on the
    next read. A warning here would be one on every `break`."""
    producer.client.append.publish("events", key=[site, "one"], value=b"1")

    with caplog.at_level("WARNING", logger="saguin"):
        with saguin.Client("leaver-" + site, durable=True) as c:
            c.start(*broker.address)
            records = c.append.consume("events", key=[site], timeout=10)
            assert next(records).payload == b"1"
            records.close()

    said = "\n".join(r.getMessage() for r in caplog.records)
    assert "neither acknowledged nor handed back" not in said, said


def test_a_job_answered_before_the_loop_is_left_says_nothing(
    producer, broker, site, caplog
):
    """**Acknowledging a job and then leaving the loop is not abandoning
    it**, and the warning has to tell those apart.

    Two different acknowledgements are in play. The reading loop sends the
    transport one when it asks for the next record, and a queue job is
    resolved by a verb of its own - so a worker that calls `queue.ack` and
    then leaves holds a record the loop never advanced past. The first
    version of the warning could not tell that from abandoning the job and
    fired on correct code: it fired on this suite's own schema test, which
    acks and returns.
    """
    producer.client.queue.publish("retried", key=[site, "j1"], value=b"one")

    with caplog.at_level("WARNING", logger="saguin"):
        with saguin.Client("acker-" + site, durable=False) as w:
            w.start(*broker.address)
            jobs = w.queue.fetch("retried", timeout=10)
            job = next(jobs)
            assert job.payload == b"one"
            w.queue.ack(job)
            jobs.close()

    said = "\n".join(r.getMessage() for r in caplog.records)
    assert "neither acknowledged nor handed back" not in said, said


def test_closing_a_reader_waits_for_the_broker_to_confirm_it(
    producer, broker, site
):
    """**Closing does not return until the broker has answered the
    UNSUBSCRIBE**, which is what makes "nothing arrives afterwards" a
    guarantee rather than a likelihood.

    Asserted on the answer itself rather than on a record failing to
    arrive. The behaviour is in a test of its own, but as a race it is a
    coin flip: without the wait, thirty rounds of publishing straight
    after close delivered two records, and five rounds inside one test
    caught the same mutation once in ten runs. The answer having arrived
    before `close` returned is the same fix, decided rather than sampled.
    """
    answered = []
    with saguin.Client("waits-" + site, durable=True) as c:
        c.start(*broker.address)
        c.on_unsubscribe = (
            lambda cl, u, mid, rc=None, props=None: answered.append(mid)
        )

        records = c.append.consume("events", key=[site], timeout=5)
        producer.client.append.publish("events", key=[site, "one"], value=b"1")
        assert next(records).payload == b"1"
        assert answered == [], "nothing has been unsubscribed yet"

        records.close()
        assert answered, (
            "close returned before the broker confirmed the unsubscribe, so a "
            "record published now is still delivered under a subscription that "
            "has not been removed"
        )


def test_a_record_buffered_before_a_dropped_link_is_not_read_twice(
    producer, broker, site
):
    """**A packet identifier belongs to the connection that issued it.**

    A record that reaches no reader waits in the unread buffer. If the
    link drops before anything reads it, the broker sends it again on the
    resumed session - and the buffered copy is still there, carrying the
    dead connection's packet identifier. Reading both hands the caller the
    record twice, and acknowledging the stale one sends a PUBACK for a
    number the live connection assigned to something else: a record
    acknowledged away from a consumer that never saw it, which is the one
    thing a position must never do.

    Driven with the operator's disconnect verb rather than by waiting for
    a real drop. Before the fix this read ``[A, B, A, B]`` and sent four
    acknowledgements, two of them from the dead connection.
    """
    generations = []
    c = saguin.Client("buffered-" + site, durable=True, session_expiry=300)
    c.on_connect = lambda cl, u, flags, rc, props=None: generations.append(
        bool(getattr(flags, "session_present", False))
    )
    try:
        c.start(*broker.address)

        # paho's own subscribe: what arrives is routed to no reader and
        # waits, which is the state this is about.
        c.subscribe("iot/{}/events/+".format(site), qos=1)
        time.sleep(0.5)
        for key, value in (("a", b"A"), ("b", b"B")):
            producer.client.append.publish("events", key=[site, key], value=value)
        for _ in range(40):
            if c._unread.qsize() >= 2:
                break
            time.sleep(0.25)
        assert c._unread.qsize() >= 2, "nothing was buffered, so nothing is proved"

        with saguin.Client("hangup-" + site, durable=False) as op:
            op.start(*broker.address)
            assert op.admin.disconnect("buffered-" + site) == "hung-up"

        for _ in range(60):
            if len(generations) >= 2:
                break
            time.sleep(0.25)
        assert len(generations) >= 2, "the client never reconnected"
        assert generations[1] is True, "the session did not survive, so the "\
            "broker had nothing to send again and this proves nothing"
        time.sleep(1.5)

        got = [r.payload for r in c.append.consume("events", key=[site], timeout=4)]
        assert got == [b"A", b"B"], (
            "each record should be read once: the copy the dead connection "
            "left is dropped, and the broker's own redelivery is what is read"
        )
    finally:
        c.close()


def test_a_record_held_when_the_link_drops_is_not_acknowledged_after_it(
    producer, broker, site
):
    """The other half of the same rule, and the one a reader hits without
    any buffering: a record yielded to the caller, the link dropping while
    it is in hand, and the loop then moving on.

    Moving on is what sends the acknowledgement - and the identifier it
    would send belongs to the connection that has gone. Asserted on
    whether the client sent one at all, because that is the whole of the
    claim: the record is not acknowledged here, it is sent again on the
    resumed session, and it is acknowledged then.
    """
    sent = []
    generations = []
    c = saguin.Client("held-" + site, durable=True, session_expiry=300)
    beneath = c.ack
    c.ack = lambda mid, qos=1: (sent.append(mid), beneath(mid, qos))[1]
    c.on_connect = lambda cl, u, flags, rc, props=None: generations.append(
        bool(getattr(flags, "session_present", False))
    )
    try:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], timeout=15)
        producer.client.append.publish("events", key=[site, "a"], value=b"A")
        assert next(records).payload == b"A"
        assert sent == [], "nothing has been acknowledged yet"

        with saguin.Client("hangup2-" + site, durable=False) as op:
            op.start(*broker.address)
            assert op.admin.disconnect("held-" + site) == "hung-up"
        for _ in range(60):
            if len(generations) >= 2:
                break
            time.sleep(0.25)
        assert len(generations) >= 2, "the client never reconnected"
        assert generations[1] is True, "the session did not survive"

        # Moving on. The record in hand came from the connection that has
        # gone, so nothing is acknowledged for it - and the broker sends
        # it again, which is what the loop is handed next.
        assert next(records).payload == b"A"
        assert sent == [], (
            "an acknowledgement was sent carrying a packet identifier from a "
            "connection that no longer exists: on the live one that number "
            "belongs to whatever it is holding, which is a record "
            "acknowledged away from a consumer that never saw it"
        )
    finally:
        c.close()


class _WidensTheGap(saguin.Client):
    """A client that holds open the gap a closing reader leaves.

    A reader that finishes stops routing its filters, then waits a whole
    round trip for the broker to confirm the unsubscribe, and only then
    puts back what it still held.  Anything the broker delivers in that
    gap reaches the unread buffer first, ahead of records older than
    itself.

    The gap is real: at full publishing rate it inverted the buffer in
    five runs out of ten.  Five in ten is a coin flip rather than an
    assertion, so this holds it open instead - one publish, waited for,
    at exactly the moment the round trip would have covered.  Nothing in
    the library changes; the moment is arranged rather than raced.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.in_the_gap = None
        self.gap_was_opened = False

    def _stop_being_sent(self, filters):
        if self.in_the_gap is not None:
            arrive, self.in_the_gap = self.in_the_gap, None
            arrive()
            self.gap_was_opened = True
        return super()._stop_being_sent(filters)


def test_a_reader_hands_back_what_it_held_ahead_of_what_arrived_later(
    make_producer, broker, site
):
    """**An append channel's records come out in the order they went in**,
    and the buffer a reader hands them back to must not reorder them.

    Before the fix the hand-back went to the tail of the buffer, so a
    record that arrived while the reader was closing sat in front of
    records published before it - and the reader that picked them up read
    ``[D, B, C]`` for a channel that was sent ``B, C, D``.  Not loss and
    not duplication: order, which is the whole of what an append channel
    promises and the one thing nothing else here would catch.
    """
    p = make_producer()
    c = _WidensTheGap("handback-" + site, durable=False)
    try:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], timeout=10)
        for key, value in (("a", b"A"), ("b", b"B"), ("c", b"C")):
            p.client.append.publish("events", key=[site, key], value=value)
        assert next(records).payload == b"A"

        def arrive():
            """One record delivered into the gap, and waited for."""
            p.client.append.publish("events", key=[site, "d"], value=b"D")
            for _ in range(80):
                if c._unread.qsize() >= 1:
                    return
                time.sleep(0.05)
            raise AssertionError(
                "nothing reached the unread buffer while the reader was "
                "closing, so the moment this is about never happened"
            )

        time.sleep(0.5)
        c.in_the_gap = arrive
        records.close()
        assert c.gap_was_opened, "the gap was never opened, so nothing is proved"

        buffered = [msg.payload for _, msg in list(c._unread.queue)]
        assert buffered == [b"B", b"C", b"D"], (
            "the buffer is in delivery order, not in the order the reader "
            "and the broker happened to reach it: got {!r}".format(buffered)
        )

        # And what the application is handed, which is the promise itself.
        again = c.append.consume("events", key=[site], timeout=5)
        assert payloads(again, 3) == [b"B", b"C", b"D"]
        again.close()
    finally:
        c.close()
