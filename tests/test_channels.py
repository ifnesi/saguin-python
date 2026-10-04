"""Addressing a channel by name.

What saguin promises, and what these measure against.  A channel is a name
and a topic filter, and the filter lives in the operator's configuration -
so a client that wants to work in channels asks the broker what one is, at
`$saguin/catalogue/<channel>`, with a Response Topic.  The answer is
written to the asking connection, so no subscription is needed and no
other client receives it.  The filter comes back **as written**: a `+`
takes any one level, a `{a,b}` level takes one of those spellings, and a
trailing `#` takes the rest or nothing.  A channel this client may not
use, and one that does not exist, are the same empty answer.
"""

import pytest

import saguin
from saguin.channels import ChannelInfo, compose, subscriptions


def a_channel(filter="iot/+/{device,sensor}/#", name="readings", type="append", pin=None):
    return ChannelInfo(name=name, type=type, filter=filter, verbs=("write", "read"), pin=pin)


# -- composing, with no broker in it ----------------------------------------


def test_a_topic_is_built_from_the_filter_and_the_key():
    info = a_channel()
    assert compose(info, ["site42", "device", "temp/1"]) == "iot/site42/device/temp/1"
    # `#` stands for no levels as well as for many, so its value may be left out.
    assert compose(info, ["site42", "sensor"]) == "iot/site42/sensor"
    assert compose(a_channel(filter="iot/hq/door"), []) == "iot/hq/door"
    # One value is spelled without a list often enough to be worth taking.
    assert compose(a_channel(filter="iot/+/door"), "site42") == "iot/site42/door"


def test_a_key_that_does_not_fit_says_what_the_filter_is():
    """Every refusal names the channel's filter, because somebody writing a
    client against a channel they did not configure needs exactly that."""
    info = a_channel()

    with pytest.raises(saguin.KeyDoesNotFit) as refused:
        compose(info, ["site42", "gadget", "x"])
    said = str(refused.value)
    assert "iot/+/{device,sensor}/#" in said
    assert "'device', 'sensor'" in said
    assert refused.value.filter == "iot/+/{device,sensor}/#"
    assert refused.value.channel == "readings"

    with pytest.raises(saguin.KeyDoesNotFit) as slashed:
        compose(info, ["site42/west", "device", "x"])
    assert "iot/+/{device,sensor}/#" in str(slashed.value)

    with pytest.raises(saguin.KeyDoesNotFit) as short:
        compose(info, ["site42"])
    assert "iot/+/{device,sensor}/#" in str(short.value)

    with pytest.raises(saguin.KeyDoesNotFit) as wild:
        compose(info, ["site42", "device", "a/+/b"])
    assert "wildcard" in str(wild.value)


def test_a_subscription_left_open_becomes_one_filter_per_alternative():
    """A `+` in a braced level would also reach topics beside the channel,
    whose records are not its."""
    info = a_channel()
    assert subscriptions(info, ["site42"]) == [
        "iot/site42/device/#",
        "iot/site42/sensor/#",
    ]
    assert subscriptions(info) == ["iot/+/device/#", "iot/+/sensor/#"]
    assert subscriptions(info, ["site42", "device"]) == ["iot/site42/device/#"]
    assert subscriptions(a_channel(filter="iot/+/events/+")) == ["iot/+/events/+"]


# -- against a running broker -----------------------------------------------


def test_the_broker_says_what_a_channel_is(producer):
    got = producer.client.channel("readings")
    assert got.name == "readings"
    assert got.type == "append"
    assert got.filter == "iot/+/{device,sensor}/#"
    assert "write" in got.verbs and "read" in got.verbs
    assert got.pin is None

    # A queue answers its pin, which is the one form it admits.
    queue = producer.client.channel("tasks")
    assert queue.type == "queue"
    assert queue.pin == "$saguin/queue/tasks"


def test_an_answer_is_asked_for_once_and_remembered(producer):
    first = producer.client.channel("readings")
    assert producer.client.channel("readings") is first, "a second call asked again"

    producer.client.forget_channel("readings")
    assert producer.client.channel("readings") is not first


def test_a_channel_the_broker_knows_nothing_about_says_both_things_it_could_mean(
    producer,
):
    with pytest.raises(saguin.UnknownChannel) as unknown:
        producer.client.channel("no-such-channel")
    said = str(unknown.value)
    assert "no such channel" in said and "roles grant nothing" in said

    # And nothing is remembered about it, so a grant arriving later is
    # picked up by asking again rather than by any invalidation rule.
    assert "no-such-channel" not in producer.client._known


def test_appending_by_channel_name_composes_the_topic(producer, reader, site):
    with reader() as r:
        r.subscribe("iot/{}/device/#".format(site))
        info = producer.client.append.publish(
            "readings",
            key=[site, "device", "temp/1"],
            value=b"21.5",
            headers=[("unit", "C")],
        )
        info.wait_for_publish(10)

        msg = saguin.Message(r.next())
        assert msg.topic == "iot/{}/device/temp/1".format(site)
        assert msg.payload == b"21.5"
        assert msg.headers["unit"] == "C"
        assert msg.id == info.saguin_id
        assert isinstance(msg.offset, int), "it landed in the channel, not in broadcast"


def test_a_write_by_name_refuses_a_key_before_anything_leaves(producer):
    with pytest.raises(saguin.KeyDoesNotFit) as refused:
        producer.client.append.publish("readings", key=["a", "gadget", "b"], value=b"x")
    assert "iot/+/{device,sensor}/#" in str(refused.value)


def test_a_verb_for_the_wrong_kind_of_channel_says_which_to_use(producer):
    """The three writes are one publish underneath, so this is the only
    thing between appending to a log and queueing work by mistake."""
    with pytest.raises(saguin.WrongChannelType) as wrong:
        producer.client.append.publish("tasks", key=["s", "1"], value=b"x")
    said = str(wrong.value)
    assert "queue channel" in said and "client.queue.publish()" in said

    with pytest.raises(saguin.WrongChannelType):
        producer.client.latest.set("readings", key=["s", "device", "x"], value=b"x")
    with pytest.raises(saguin.WrongChannelType):
        producer.client.queue.publish("readings", key=["s", "device", "x"], value=b"x")


def test_a_latest_channel_is_a_key_value_store(producer, site):
    """set, get and delete, and an absent key is None - the same answer a
    deleted one gives, as everywhere else on this channel type."""
    c = producer.client.latest
    assert c.get("state", key=[site, "temp"]) is None

    c.set("state", key=[site, "temp"], value=b"18").wait_for_publish(10)
    assert c.get("state", key=[site, "temp"]) == b"18"

    c.set("state", key=[site, "temp"], value=b"19").wait_for_publish(10)
    assert c.get("state", key=[site, "temp"]) == b"19"

    c.delete("state", key=[site, "temp"]).wait_for_publish(10)
    assert c.get("state", key=[site, "temp"]) is None


def test_a_worker_answers_for_its_own_job(producer, broker, site):
    """job_ack resolves the record; job_return hands it back and spends
    the attempt. Both are answered on the connection the job arrived on,
    which is the only session the broker takes an answer from."""
    with saguin.Client("acker-" + site, durable=True) as c:
        c.start(*broker.address)
        jobs = c.queue.fetch("tasks", timeout=15)
        producer.client.queue.publish("tasks", key=[site, "1"], value=b"do it")
        job = next(jobs)
        assert job.attempt == 1
        c.queue.ack(job).wait_for_publish(10)

    # Acknowledged work is gone: a second worker is offered nothing.
    with saguin.Client("second-" + site, durable=True) as later:
        later.start(*broker.address)
        with pytest.raises(StopIteration):
            next(later.queue.fetch("tasks", timeout=6))


def test_publishing_a_topic_still_works_as_paho_does(producer, reader, site):
    """`publish` is MQTT's own verb and is left alone: a topic and a
    payload. Broadcast is plain MQTT, and the saguin verbs sit beside it."""
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        producer.publish("iot/{}/events/thing".format(site), b"x")
        assert saguin.Message(r.next()).payload == b"x"


def test_consuming_by_name_subscribes_to_every_alternative(producer, broker, site):
    producer.publish("iot/{}/device/temp".format(site), b"1")
    producer.publish("iot/{}/sensor/humidity".format(site), b"2")

    with saguin.Client("by-name-" + site, durable=True) as c:
        c.start(*broker.address)
        got = []
        for record in c.append.consume("readings", key=[site], timeout=15):
            got.append((record.topic, record.payload))
            if len(got) == 2:
                break

    assert sorted(payload for _, payload in got) == [b"1", b"2"]
    assert {topic for topic, _ in got} == {
        "iot/{}/device/temp".format(site),
        "iot/{}/sensor/humidity".format(site),
    }


def test_a_queue_is_fetched_through_its_pin_and_not_narrowed(producer, broker, site):
    """A queue admits one subscription form and no other: two spellings
    would be two consumer groups, each taking a copy of every job. So
    `fetch` takes no key at all, and asking for one is a TypeError from
    the method rather than a silent second group."""
    with saguin.Client("worker-" + site, durable=True) as c:
        c.start(*broker.address)
        with pytest.raises(TypeError):
            c.queue.fetch("tasks", key=[site])

        records = c.queue.fetch("tasks", timeout=15)
        producer.publish("work/{}/jobs/1".format(site), b"do it")
        job = next(records)

    assert job.payload == b"do it"
    assert job.attempt == 1


def test_the_callers_own_paho_callbacks_still_run(producer, broker, site):
    """The library keeps a handler of its own in front of `on_connect`,
    `on_subscribe` and `on_publish`, and the caller's is called after it.

    **Driven because the first version of it recursed.** paho's callback
    properties are backed by `_on_connect`, `_on_subscribe` and
    `_on_publish`; storing the caller's under those very names made the
    handler its own caller. Nothing about the names says so, which is why
    this is a test rather than a comment.
    """
    seen = {"connect": 0, "subscribe": 0, "publish": 0}

    c = saguin.Client("callbacks-" + site, durable=True)
    c.on_connect = lambda cl, u, f, rc, p: seen.__setitem__(
        "connect", seen["connect"] + 1
    )
    c.on_subscribe = lambda cl, u, mid, codes, p: seen.__setitem__(
        "subscribe", seen["subscribe"] + 1
    )
    c.on_publish = lambda cl, u, mid, rc, p: seen.__setitem__(
        "publish", seen["publish"] + 1
    )

    with c:
        c.start(*broker.address)
        producer.client.append.publish("events", key=[site, "thing"], value=b"hello")
        record = next(c.append.consume("events", key=[site], timeout=15))
        c.append.publish("events", key=[site, "back"], value=b"answer")

    assert record.payload == b"hello"
    assert c.session_present is False
    assert seen["connect"] == 1
    assert seen["subscribe"] == 1
    assert seen["publish"] >= 1


def test_a_reader_takes_its_records_and_on_message_keeps_the_rest(
    producer, broker, site
):
    """**Worth a test because the obvious belief is wrong.** While a
    `consume` loop is reading a filter, records matching it go to that
    reader and do **not** reach the caller's `on_message` - paho calls a
    topic callback instead of `on_message`, not as well as it.

    That is the whole point of the routing: before it, every reader drew
    from one queue per client and two readers took each other's records.
    What `on_message` keeps is everything no reader is reading.
    """
    import queue as q

    seen = q.Queue()
    with saguin.Client("mixed-" + site, durable=True) as c:
        c.start(*broker.address)
        c.on_message = lambda cl, u, msg: seen.put(msg)
        c.subscribe("broadcast/{}/#".format(site), qos=1)

        records = c.append.consume("events", key=[site], timeout=8)
        producer.client.append.publish("events", key=[site, "thing"], value=b"channel")
        producer.publish("broadcast/{}/shout".format(site), b"broadcast")

        # The channel record goes to the reader...
        assert next(records).payload == b"channel"
        # ...and the broadcast, which no reader is reading, to on_message.
        assert seen.get(timeout=10).payload == b"broadcast"
        assert seen.empty(), "on_message was also given the channel's record"


def test_a_client_with_no_position_starts_where_start_says(producer, broker, site):
    """`start` is the broker's own vocabulary and is passed through: an
    integer offset, or a string holding a duration or an RFC 3339 moment."""
    for n in (b"1", b"2", b"3"):
        producer.client.append.publish("events", key=[site, "thing"], value=n)

    # -1 is the channel's next offset: only what arrives after this.
    with saguin.Client("tail-" + site, durable=True) as c:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], start=-1, timeout=6)
        producer.client.append.publish("events", key=[site, "thing"], value=b"4")
        assert next(records).payload == b"4"

    # 0 is the retention floor: everything still held.
    with saguin.Client("floor-" + site, durable=True) as c:
        c.start(*broker.address)
        got = []
        for record in c.append.consume("events", key=[site], start=0, timeout=10):
            got.append(record.payload)
            if len(got) == 4:
                break
        assert got == [b"1", b"2", b"3", b"4"]


def test_start_applies_once_and_not_on_every_restart(producer, broker, site):
    """The value is written once in the code, so a `start` that seeked
    every time would replay the whole channel on every restart. It applies
    only where the broker has no session for this client id - which is
    what tells the SDK there is no position.

    Each read is drained to its timeout rather than broken out of, because
    the iterator acknowledges a record when the loop asks for the next
    one: leaving early leaves the last record unacknowledged, and it
    rightly comes back.
    """
    name = "once-" + site
    for n in (b"1", b"2"):
        producer.client.append.publish("events", key=[site, "thing"], value=n)

    with saguin.Client(name, durable=True) as first:
        first.start(*broker.address)
        assert first.session_present is False
        got = [r.payload for r in first.append.consume(
            "events", key=[site], start=0, timeout=4)]
        assert got == [b"1", b"2"]

    producer.client.append.publish("events", key=[site, "thing"], value=b"3")

    with saguin.Client(name, durable=True) as again:
        again.start(*broker.address)
        assert again.session_present is True
        # start=0 again, and it does not replay: the position is honoured.
        got = [r.payload for r in again.append.consume(
            "events", key=[site], start=0, timeout=4)]
        assert got == [b"3"], "start fired a second time and replayed the channel"


def test_seek_moves_the_position_every_time(producer, broker, site):
    """Which is what makes it a different verb from `start`."""
    name = "seeker-" + site
    for n in (b"1", b"2"):
        producer.client.append.publish("events", key=[site, "thing"], value=n)

    with saguin.Client(name, durable=True) as c:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], timeout=10)
        first_record = next(records)
        assert [first_record.payload, next(records).payload] == [b"1", b"2"]

        # **Seeking answers the offset it landed on**, which is asserted
        # against a record's own rather than against ">= 0": a seek that
        # answered a constant would pass that and tell a caller nothing.
        first_offset = first_record.offset
        assert c.append.seek("events", first_offset) == first_offset
        replayed = next(c.append.consume("events", key=[site], timeout=10))
        assert replayed.payload == b"1"
        assert replayed.offset == first_offset

        # And the floor is the lowest the channel still holds, which is at
        # or below the first record this test wrote.
        assert c.append.seek("events", 0) <= first_offset


def test_seek_takes_a_time_as_well_as_an_offset(producer, broker, site):
    producer.client.append.publish("events", key=[site, "thing"], value=b"recent")

    with saguin.Client("timeseeker-" + site, durable=True) as c:
        c.start(*broker.address)
        # A duration means ago, sign or no sign.
        c.append.seek("events", "12h")
        assert next(
            c.append.consume("events", key=[site], timeout=10)
        ).payload == b"recent"


def test_seeking_needs_a_client_that_keeps_a_place(broker, site):
    with saguin.Client("placeless-" + site) as c:
        c.start(*broker.address)
        with pytest.raises(ValueError) as refused:
            c.append.seek("events", 0)
        assert "keeps no place" in str(refused.value)


def test_a_refused_request_carries_the_brokers_own_sentence(broker, site):
    """Every question rides a QoS 1 publish, and what is wrong with one
    comes back in the PUBACK - never on the reply topic, which carries an
    answer and nothing else. Waiting for a reply that will never come
    would turn the broker's sentence into a timeout saying nothing."""
    with saguin.Client("bad-seek-" + site, durable=True) as c:
        c.start(*broker.address)
        with pytest.raises(saguin.RequestRefused) as refused:
            c.append.seek("events", "not-a-position")
        assert refused.value.reason_code.is_failure
        assert refused.value.reason_string


def test_following_a_latest_channel_gives_state_then_changes(producer, broker, site):
    """A `latest` channel answers a new subscription with the current value
    of everything the filter reaches, and every change after that.
    `is_catch_up` is what tells the two apart."""
    producer.client.latest.set("state", key=[site, "temp"], value=b"18")

    with saguin.Client("follower-" + site, durable=True) as c:
        c.start(*broker.address)
        records = c.latest.consume("state", key=[site], timeout=10)

        state = next(records)
        assert state.payload == b"18"
        assert state.is_catch_up is True, "the state a subscriber arrives to"

        producer.client.latest.set("state", key=[site, "temp"], value=b"19")
        change = next(records)
        assert change.payload == b"19"
        assert change.is_catch_up is False, "a change that has just happened"
        assert change.offset > state.offset


def test_hanging_up_another_client_and_a_client_that_is_not_there(
    producer, broker, site
):
    """The two answers are worth telling apart, which is why there is a
    reply at all: otherwise "that device went away an hour ago" and "you
    have misspelled the id" read the same."""
    victim = saguin.Client("victim-" + site)
    victim.start(*broker.address)
    try:
        assert producer.client.admin.disconnect("victim-" + site) == "hung-up"
    finally:
        victim.loop_stop()

    assert producer.client.admin.disconnect("nobody-" + site) == "no-such-client"


def test_a_worker_acks_what_succeeded_and_hands_back_what_raised(
    producer, broker, site
):
    """The shape RabbitMQ's clients and the frameworks over them have: a
    job that raises is handed back and **the worker carries on**. One that
    stopped on a bad job would stop everything behind it, which is worse
    than the job that failed.

    **Seen through the attempt count, which is where it shows.** A handed
    back job comes straight back - to this same worker, since it is the
    one asking - so what proves the nack is the second offer arriving as
    attempt 2. Watching for it on another client instead is watching the
    wrong place: by the time that client looked, a handler that always
    raises had already burned the job's attempts.
    """
    producer.client.queue.publish("retried", key=[site, "flaky"], value=b"flaky")
    producer.client.queue.publish("retried", key=[site, "fine"], value=b"fine")

    attempts, failures = [], []

    def handler(job):
        attempts.append((job.payload, job.attempt))
        if job.payload == b"flaky" and job.attempt == 1:
            raise RuntimeError("cannot pack it")

    with saguin.Client("worker-" + site, durable=True) as w:
        w.start(*broker.address)
        handled = w.queue.work(
            "retried", handler,
            on_error=lambda job, exc: failures.append((job.payload, str(exc))),
            timeout=8,
        )

    # The failing job came back as attempt 2 and was taken again.
    assert (b"flaky", 1) in attempts and (b"flaky", 2) in attempts
    # The one that returned normally was offered exactly once.
    assert [a for a in attempts if a[0] == b"fine"] == [(b"fine", 1)]
    assert handled == 3, "two jobs, one of them twice"
    assert failures == [(b"flaky", "cannot pack it")]

    # Both are resolved now, so nothing is left for anybody else.
    with saguin.Client("second-" + site, durable=True) as later:
        later.start(*broker.address)
        assert [job.payload for job in later.queue.fetch("retried", timeout=5)] == []


def test_a_failing_job_is_logged_when_nobody_is_watching(producer, broker, site, caplog):
    """With no `on_error`, the failure goes to a log rather than nowhere: a
    queue that fails every job with nothing anywhere saying so is what this
    is written to avoid."""
    producer.client.queue.publish("retried", key=[site, "1"], value=b"boom")

    def handler(job):
        raise RuntimeError("it went wrong")

    with saguin.Client("quiet-" + site, durable=True) as w:
        w.start(*broker.address)
        with caplog.at_level("ERROR", logger="saguin"):
            w.queue.work("retried", handler, timeout=6)

    assert any("it went wrong" in r.getMessage() for r in caplog.records), (
        "a job that raised was handed back with nothing written anywhere"
    )


def test_a_handed_back_job_waits_longer_each_time(producer, broker, site):
    """`linear` backoff is base x attempt, so the gaps grow: 1s, 2s, 3s.

    Asserted as *growing* rather than as exact numbers, because a gap is
    the broker's floor and not its promise - the queue offers on a 200ms
    tick, so every gap is that much or more. What would be wrong is a gap
    that did not grow, or none at all.
    """
    import time

    producer.client.queue.publish("backoff", key=[site, "1"], value=b"never works")
    seen = []

    def handler(job):
        seen.append((job.attempt, time.monotonic()))
        raise RuntimeError("no")

    with saguin.Client("backoff-" + site, durable=True) as w:
        w.start(*broker.address)
        w.queue.work("backoff", handler, on_error=lambda job, exc: None, timeout=12)

    assert [attempt for attempt, _ in seen] == [1, 2, 3, 4], (
        "want four attempts and no more, since max_attempts is 4; got {}".format(seen)
    )
    gaps = [b - a for (_, a), (_, b) in zip(seen, seen[1:])]
    assert gaps[0] >= 0.9 and gaps[1] >= 1.9 and gaps[2] >= 2.9, (
        "gaps were {}, want about 1s, 2s and 3s".format(gaps)
    )
    assert gaps[0] < gaps[1] < gaps[2], "the gap did not grow: {}".format(gaps)

    # And with its attempts spent, the job is dead-lettered rather than
    # offered a fifth time.
    with saguin.Client("backoff-dlq-" + site, durable=True) as c:
        c.start(*broker.address)
        dead = next(c.append.consume("backoff__dlq", key=[site], timeout=15))
    assert dead.payload == b"never works"
    assert dead.dlq.reason == "attempts_exhausted"
    assert dead.dlq.attempts == 4


def test_two_workers_share_a_queue_and_neither_sees_the_others_work(
    producer, broker, site
):
    """A queue hands each job to one worker. Both workers are proved to
    have done some of it, because a test where one did everything would
    pass while measuring nothing about sharing."""
    import threading

    jobs = [str(n).encode() for n in range(8)]
    for one in jobs:
        producer.client.queue.publish(
            "retried", key=[site, one.decode()], value=one
        )

    taken = {}
    lock = threading.Lock()

    def run(name):
        with saguin.Client(name, durable=True) as w:
            w.start(*broker.address)

            def handler(job):
                with lock:
                    taken.setdefault(job.payload, []).append(name)

            w.queue.work("retried", handler, timeout=6)

    names = ["worker-a-" + site, "worker-b-" + site]
    threads = [threading.Thread(target=run, args=(n,)) for n in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert sorted(taken) == sorted(jobs), "not every job was handed out: {}".format(
        sorted(taken)
    )
    doubled = {job: who for job, who in taken.items() if len(who) > 1}
    assert not doubled, "a job went to more than one worker: {}".format(doubled)
    did = {name for who in taken.values() for name in who}
    assert did == set(names), (
        "only {} did any work, so this measured nothing about sharing".format(did)
    )


def test_a_dead_letter_can_be_put_back_on_its_queue(producer, reader, broker, site):
    """Read then republish, done here rather than by the broker, because
    deciding that failed work should be tried again is a judgement nobody
    but the operator can make."""
    sent = producer.client.queue.publish("tasks", key=[site, "1"], value=b"do it")
    sent.wait_for_publish(10)

    # Fail it once, which exhausts `tasks` and dead-letters it.
    with reader() as worker:
        worker.subscribe("$saguin/queue/tasks", qos=1)
        worker.answer(worker.next(), "return")

    with saguin.Client("redriver-" + site, durable=True) as c:
        c.start(*broker.address)
        dead = next(c.append.consume("tasks__dlq", key=[site], timeout=30))
        assert dead.dlq.reason == "attempts_exhausted"

        put_back = c.queue.redrive("tasks", dead)
        put_back.wait_for_publish(10)

    # **It is on the queue again, as attempt 1 and with its own id.** The
    # id is what makes redriving twice something a consumer can notice
    # rather than a second piece of work.
    with saguin.Client("after-" + site, durable=True) as worker:
        worker.start(*broker.address)
        again = next(worker.queue.fetch("tasks", timeout=15))
        assert again.payload == b"do it"
        assert again.topic == "work/{}/jobs/1".format(site)
        assert again.id == sent.saguin_id
        assert again.attempt == 1
        assert again.dlq is None, "the broker's dead-letter account did not travel"


def test_redrive_refuses_a_record_that_is_not_a_dead_letter(producer, broker, site):
    """A topic without `__dlq` where the filter puts it is not a record
    this rule describes - and republishing it unchanged would put the work
    straight back into the dead-letter channel it came from."""
    producer.client.append.publish("events", key=[site, "thing"], value=b"ordinary")

    with saguin.Client("norediver-" + site, durable=True) as c:
        c.start(*broker.address)
        ordinary = next(c.append.consume("events", key=[site], timeout=10))
        with pytest.raises(ValueError) as refused:
            c.queue.redrive("tasks", ordinary)

    said = str(refused.value)
    assert "__dlq" in said and "work/+/jobs/+/__dlq" in said, said


def test_redrive_refuses_a_channel_that_is_not_a_queue(producer, broker, site):
    """The queue is named, not the dead-letter channel, so naming anything
    else is caught by this namespace's own check before the record is even
    looked at."""
    with saguin.Client("wrongchan-" + site, durable=True) as c:
        c.start(*broker.address)
        producer.client.append.publish("events", key=[site, "thing"], value=b"x")
        record = next(c.append.consume("events", key=[site], timeout=10))
        with pytest.raises(saguin.WrongChannelType) as refused:
            c.queue.redrive("events", record)
    assert "queue" in str(refused.value)


def test_the_dlq_level_comes_off_where_the_filter_puts_it(producer, reader, broker, site):
    """**The case the whole design is for.** A queue filtered `bulk/#` has
    its dead letters at `bulk/__dlq/...`, so `__dlq` is the *second* level
    and not the last. Stripping the last level - the obvious
    implementation - would put the work back on the wrong topic, or on no
    channel at all.
    """
    with saguin.Client("bulk-check-" + site, durable=True) as c:
        c.start(*broker.address)
        assert c.channel("bulk__dlq").filter == "bulk/__dlq/#", (
            "the broker puts __dlq somewhere else than this test assumes"
        )

    sent = producer.client.queue.publish("bulk", key=["{}/deep/1".format(site)],
                                         value=b"heavy")
    sent.wait_for_publish(10)

    with reader() as worker:
        worker.subscribe("$saguin/queue/bulk", qos=1)
        worker.answer(worker.next(), "return")

    with saguin.Client("bulk-redriver-" + site, durable=True) as c:
        c.start(*broker.address)
        dead = next(c.append.consume("bulk__dlq", key=["{}/deep/1".format(site)],
                                     timeout=30))
        assert dead.topic == "bulk/__dlq/{}/deep/1".format(site)
        c.queue.redrive("bulk", dead).wait_for_publish(10)

    with saguin.Client("bulk-after-" + site, durable=True) as worker:
        worker.start(*broker.address)
        again = next(worker.queue.fetch("bulk", timeout=15))
        assert again.topic == "bulk/{}/deep/1".format(site), (
            "the level came off in the wrong place"
        )
        assert again.payload == b"heavy"
        assert again.id == sent.saguin_id


def test_two_readers_on_one_client_do_not_take_each_others_records(
    producer, broker, site
):
    """**Found by the guided tour, not by this suite.** Every reader used
    to draw from one queue of arrivals per client, so whichever asked for
    the next record first was handed whatever had turned up - whatever it
    had subscribed to. In the tour, a `latest` value arrived at a
    `queue.fetch` and was reported as a job.

    It is not a tidiness problem. The reader then acknowledges a record
    belonging to the other subscription: a record acknowledged away from a
    consumer that never saw it.
    """
    producer.client.latest.set("state", key=[site, "door"], value=b"shut")

    with saguin.Client("two-readers-" + site, durable=True) as c:
        c.start(*broker.address)

        state = c.latest.consume("state", key=[site], timeout=6)
        jobs = c.queue.fetch("retried", timeout=6)

        producer.client.queue.publish("retried", key=[site, "1"], value=b"a job")

        # Each reader is handed what it subscribed to, and neither is
        # handed the other's.
        job = next(jobs)
        assert job.payload == b"a job", "the queue reader was given {!r}".format(
            job.payload
        )
        assert job.topic.startswith("again/"), job.topic
        c.queue.ack(job)

        value = next(state)
        assert value.payload == b"shut", "the state reader was given {!r}".format(
            value.payload
        )
        assert "/state/" in value.topic, value.topic


def test_a_reader_that_is_let_go_stops_being_sent_anything(producer, broker, site):
    """Closing a reader stops the routing **and** the subscription.

    **The second half was added after this test was written**, to fix a
    queue handing leases to a reader that had finished - so this used to
    assert that the caller's `on_message` picked the traffic up instead,
    and now nothing arrives at all. That is the behaviour to want: a
    client that has stopped reading a channel should not be told about it.

    Subscribing again with paho's own verb proves the records were there
    to be had, so this is "not subscribed" rather than "broker went
    quiet".

    **Several rounds, because one is a coin flip.** Closing used to send
    the UNSUBSCRIBE and return without waiting for the answer, so a
    record published in that gap was still delivered under a
    subscription the broker had not yet removed. One round passed on its
    own and failed in a full suite; thirty rounds of it caught the race
    twice. Closing now waits, and the rounds are what would notice if it
    stopped.
    """
    import queue as q
    import uuid

    # **A round of its own each time**, client and topics both. One
    # durable client over five rounds would be served the record the
    # previous round left unacknowledged, which is the position doing
    # exactly what it promises and has nothing to do with what is being
    # asked here.
    for round in range(5):
        own = uuid.uuid4().hex[:12]
        seen = q.Queue()
        with saguin.Client("letgo-{}-{}".format(site, round), durable=True) as c:
            c.start(*broker.address)
            c.on_message = lambda cl, u, msg: seen.put(msg)

            records = c.append.consume("events", key=[own], timeout=5)
            producer.client.append.publish("events", key=[own, "one"], value=b"1")
            assert next(records).payload == b"1"
            records.close()

            # Nothing reaches the reader, and nothing reaches on_message
            # either: the subscription went with it, and it had gone
            # before close returned.
            producer.client.append.publish("events", key=[own, "two"], value=b"2")
            with pytest.raises(q.Empty):
                seen.get(timeout=1)

    # The last round's client is gone; this one proves the record was
    # there to be had, so the silence above is "not subscribed" rather
    # than "the broker went quiet".
    seen = q.Queue()
    with saguin.Client("letgo-proof-" + site, durable=True) as c:
        c.start(*broker.address)
        c.on_message = lambda cl, u, msg: seen.put(msg)

        c.subscribe("iot/{}/events/+".format(site), qos=1)
        producer.client.append.publish("events", key=[site, "three"], value=b"3")
        assert seen.get(timeout=10).payload == b"3"


def test_a_resumed_session_is_read_by_the_reader_it_belongs_to(
    producer, broker, site
):
    """**The defect a rename review turned up.** A durable client that
    reconnects is served records for subscriptions made on its *previous*
    connection - before the application calls `consume`, so before any
    route for them exists. They were dropped, which is exactly what a
    durable consumer reconnects for.

    They wait with everything else that reached no reader, and `consume`
    sweeps out the ones that are its own before it subscribes.
    """
    name = "resumer-sweep-" + site
    producer.client.append.publish("events", key=[site, "one"], value=b"1")

    with saguin.Client(name, durable=True) as first:
        first.start(*broker.address)
        assert next(first.append.consume("events", key=[site], timeout=10)).payload == b"1"

    # Published while nobody is connected, so the resumed session is what
    # carries it - and it arrives before `consume` is called.
    producer.client.append.publish("events", key=[site, "two"], value=b"2")

    with saguin.Client(name, durable=True) as again:
        again.start(*broker.address)
        assert again.session_present is True
        import time

        time.sleep(0.5)  # let the resume deliver before anything reads
        got = [r.payload for r in again.append.consume("events", key=[site], timeout=4)]

    assert b"2" in got, "a resumed session's records reached no reader: {}".format(got)


def test_a_second_reader_of_one_filter_takes_over_and_keeps_the_records(
    producer, broker, site
):
    """Reading a channel, seeking, and reading it again is ordinary - so
    the second reader takes the filter over rather than being refused.
    What the first was still holding goes back, or the records the broker
    already sent on this connection are lost."""
    producer.client.append.publish("events", key=[site, "one"], value=b"1")
    producer.client.append.publish("events", key=[site, "two"], value=b"2")

    with saguin.Client("takeover-" + site, durable=True) as c:
        c.start(*broker.address)
        first = c.append.consume("events", key=[site], timeout=10)
        assert next(first).payload == b"1"

        # A second reader of the same filter. The first is closed.
        second = c.append.consume("events", key=[site], timeout=10)
        assert next(second).payload == b"2", "the record the first held was dropped"

        with pytest.raises(StopIteration):
            next(first)


# Every verb that names a channel, and a channel of the wrong kind for it.
# Built as a list rather than generated, because what is being checked is
# that each verb is wired to the right type - which is the thing a
# generator would have to be told anyway.
WRONG_CHANNEL = [
    ("append", "publish", lambda ns, s: ns.publish("tasks", key=[s, "1"], value=b"x")),
    ("append", "consume", lambda ns, s: next(ns.consume("state", key=[s], timeout=2))),
    ("append", "seek", lambda ns, s: ns.seek("state", 0)),
    ("latest", "set", lambda ns, s: ns.set("events", key=[s, "b"], value=b"x")),
    ("latest", "get", lambda ns, s: ns.get("tasks", key=[s, "1"])),
    ("latest", "delete", lambda ns, s: ns.delete("events", key=[s, "b"])),
    ("latest", "consume", lambda ns, s: next(ns.consume("tasks", timeout=2))),
    ("queue", "publish", lambda ns, s: ns.publish("events", key=[s, "b"], value=b"x")),
    ("queue", "fetch", lambda ns, s: next(ns.fetch("state", timeout=2))),
    ("queue", "work", lambda ns, s: ns.work("events", lambda job: None, timeout=2)),
    ("queue", "redrive", lambda ns, s: ns.redrive("state", None)),
]


@pytest.mark.parametrize("group,verb,call", WRONG_CHANNEL,
                         ids=["{}.{}".format(g, v) for g, v, _ in WRONG_CHANNEL])
def test_every_verb_refuses_the_wrong_kind_of_channel(broker, site, group, verb, call):
    """The three writes are one publish underneath and the reads are one
    subscribe, so this check is the only thing between appending to a log
    and queueing work by mistake. It has to be on **every** verb that
    names a channel, not on the ones somebody remembered."""
    with saguin.Client("wrong-{}-{}-{}".format(group, verb, site), durable=True) as c:
        c.start(*broker.address)
        with pytest.raises(saguin.WrongChannelType) as refused:
            call(getattr(c, group), site)

    said = str(refused.value)
    assert "channel" in said and "client.{}.".format(group) not in said.split("use")[0]
    # It names what to use instead, and that advice is a real verb.
    assert "use client." in said, said


def test_no_verb_that_names_a_channel_is_left_unchecked():
    """**The list above is only worth having if it is complete.** A verb
    added to a namespace without a case here would be a verb nobody
    proved was wired to the right channel type - so the namespaces are
    read, and anything not covered fails this."""
    from saguin import verbs

    # ack and nack answer a job rather than name a channel: the job
    # carries where to reply, so there is no channel to be wrong about.
    answers_a_job = {"ack", "nack"}
    covered = {(g, v) for g, v, _ in WRONG_CHANNEL}
    for group, cls in (("append", verbs.Append), ("latest", verbs.Latest),
                       ("queue", verbs.Queue)):
        public = {
            name for name in vars(cls)
            if not name.startswith("_") and callable(vars(cls)[name])
        } - answers_a_job
        missing = {(group, v) for v in public} - covered
        assert not missing, "no wrong-channel case for {}".format(sorted(missing))


def test_a_finished_reader_stops_being_sent_work(producer, broker, site):
    """**A reader that has finished used to stay subscribed**, so the
    broker went on handing it records. On a queue that is worse than
    untidy: a queue hands out *leases*, so jobs went to a client that
    would never answer, timed out, were retried and would be
    dead-lettered for no reason anybody could see.

    Found by the guided tour: two workers were handed four of six jobs,
    and the missing two had gone to a worker from an earlier section whose
    loop had already ended.
    """
    with saguin.Client("finished-" + site, durable=True) as spent:
        spent.start(*broker.address)
        # A reader that runs out and ends, leaving nothing routed.
        assert list(spent.queue.fetch("retried", timeout=3)) == []

        # Now somebody else takes the queue up.
        with saguin.Client("working-" + site, durable=True) as busy:
            busy.start(*broker.address)
            jobs = busy.queue.fetch("retried", timeout=8)

            wanted = [b"a", b"b", b"c", b"d"]
            for one in wanted:
                producer.client.queue.publish(
                    "retried", key=[site, one.decode()], value=one
                ).wait_for_publish(10)

            got = []
            for job in jobs:
                got.append(job.payload)
                busy.queue.ack(job)
                if len(got) == len(wanted):
                    break

    assert sorted(got) == wanted, (
        "jobs went somewhere else - a finished reader is still being sent "
        "work: {}".format(sorted(got))
    )


def test_ten_workers_share_a_queue_and_no_job_is_handed_out_twice(
    producer, broker, site
):
    """**Two workers is enough to show a job going to the wrong one; it
    says nothing about ten.**

    Every record-routing defect fixed on 2026-09-04 - per-reader routing,
    the unread buffer, a finished reader unsubscribing, and when that
    unsubscribe takes effect - was found with two.  What this asserts is
    the queue's own promise rather than throughput: every job handed out,
    every job handed to exactly one worker, and every one finished once.

    `backoff` rather than `retried`, because its visibility timeout is
    thirty seconds: a lease that expired under load would put a job back
    legitimately, and a test that cannot tell that from a defect asserts
    nothing.
    """
    import threading

    jobs = ["job-{:02d}".format(n) for n in range(30)]

    taken = {}
    lock = threading.Lock()
    asking = threading.Barrier(11)

    def run(name):
        with saguin.Client(name, durable=True) as w:
            w.start(*broker.address)

            def handler(job):
                # Every handing, not the last one per job: a dictionary of
                # job -> worker would overwrite one name with the other,
                # and a job that went to two workers would read the same
                # as a job that went to one. `work` acknowledges a handler
                # that returns, so a handing counted here is also a job
                # finished.
                with lock:
                    taken.setdefault(job.payload.decode(), []).append(name)

            asking.wait(timeout=60)
            w.queue.work("backoff", handler, timeout=8)

    names = ["ten-{}-{}".format(n, site) for n in range(10)]
    threads = [threading.Thread(target=run, args=(n,)) for n in names]
    for t in threads:
        t.start()

    # **Published after all ten are up, not before.** Publishing first
    # lets a worker that connects late find the queue already empty, and
    # then "one worker got nothing" means the test was slow rather than
    # that the queue stopped spreading work - which is the one thing this
    # is here to notice.
    asking.wait(timeout=60)
    for one in jobs:
        producer.client.queue.publish(
            "backoff", key=[site, one], value=one.encode()
        ).wait_for_publish(10)

    for t in threads:
        t.join(timeout=120)

    assert sorted(taken) == sorted(jobs), (
        "not every job was handed out - missing {}".format(
            sorted(set(jobs) - set(taken)))
    )
    twice = {job: who for job, who in taken.items() if len(who) > 1}
    assert not twice, "a job was handed to more than one worker: {}".format(twice)

    busy = {name for who in taken.values() for name in who}
    # **All ten, not a floor.** A floor of two would be satisfied by a
    # queue that had collapsed to two workers, which is the regression
    # worth catching here. Measured before it was asserted: twelve runs,
    # eight of them under six busy CPUs, every one of them ten of ten.
    assert len(busy) == 10, (
        "{} of ten workers got any of the thirty jobs: {}".format(
            len(busy), sorted(busy))
    )


def test_a_job_a_departing_worker_was_holding_is_not_stranded(
    producer, broker, site
):
    """A worker that leaves without answering holds its job until it
    disconnects, and then the queue offers it again.

    **The worker holds one job, and that is what makes this about one.**
    The broker offers a worker one job from a queue at a time, so a probe
    that takes one job and leaves holds that one and nothing more.
    Receive Maximum 1 stays as a second guard: against a broker that
    handed out prefetch, a reader that has not yielded the rest to its
    caller would be leased the whole backlog, and the queue would look
    stalled for everybody.

    Two phases, because the promise has two halves: while the leaver is
    connected the other eleven jobs still flow, and the twelfth comes back
    once it has gone.
    """
    import threading

    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties

    jobs = ["left-{:02d}".format(n) for n in range(12)]
    for one in jobs:
        producer.client.queue.publish(
            "backoff", key=[site, one], value=one.encode()
        ).wait_for_publish(10)

    one_at_a_time = Properties(PacketTypes.CONNECT)
    one_at_a_time.ReceiveMaximum = 1
    leaver = saguin.Client("leaver-" + site, durable=True)
    leaver.start(*broker.address, properties=one_at_a_time)
    offers = leaver.queue.fetch("backoff", timeout=10)
    abandoned = next(offers).payload.decode()
    assert abandoned in jobs

    def pool(names, timeout):
        got = {}
        lock = threading.Lock()

        def run(name):
            with saguin.Client(name, durable=True) as w:
                w.start(*broker.address)

                def handler(job):
                    with lock:
                        got.setdefault(job.payload.decode(), []).append(name)

                w.queue.work("backoff", handler, timeout=timeout)

        threads = [threading.Thread(target=run, args=(n,)) for n in names]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)
        return got

    # One job is held, and the rest of the queue is nobody else's problem.
    while_it_holds = pool(["during-{}-{}".format(n, site) for n in range(4)], 8)
    assert sorted(while_it_holds) == sorted(set(jobs) - {abandoned}), (
        "the eleven jobs the departing worker was not holding did not reach "
        "anybody: got {}".format(sorted(while_it_holds))
    )
    assert abandoned not in while_it_holds, (
        "{!r} was offered to somebody else while the worker holding it was "
        "still connected - two workers with the same job is the one thing a "
        "queue may not do".format(abandoned)
    )

    # And it goes away without answering.
    offers.close()
    leaver.close()

    after = pool(["after-{}-{}".format(n, site) for n in range(4)], 10)
    assert abandoned in after, (
        "{!r} was never offered to anybody else after the worker holding it "
        "disconnected: it is stranded, and the queue is one job short with "
        "nothing to show for it".format(abandoned)
    )
    again = {job: who for job, who in after.items() if len(who) > 1}
    assert not again, "a job was handed out more than once: {}".format(again)
