"""Taking a slice of a channel: what the SDK puts on the SUBSCRIBE.

**The oracle is the hash, and the hash is checked against the document
rather than against the broker.**  RFC 0003 writes FNV-1a-64 out in full
with four worked vectors precisely so that a client can work out its own
share without asking anybody, so the first test here recomputes those
vectors and every other test uses the hash to say which member *should*
have received a record.  Without that first test the rest would be the
library agreeing with itself.

Records are counted across the group rather than per member.  A split that
loses one record and a split that delivers one twice both leave every
member looking healthy, and only the arithmetic over all of them shows it.
"""

import queue
import threading

import pytest
import saguin
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from saguin.channels import declarations

# `0x83` - the code the broker refuses a declaration with, whether the
# declaration is malformed or lands somewhere a slice may not be taken.
NOT_AUTHORIZED = 131


def declaring(*values):
    """SUBSCRIBE properties carrying these `saguin-filter` calls verbatim.

    Written out here rather than borrowed from the library, because the
    tests that use it are asking what the *broker* does with a spelling
    the library refuses before sending one.
    """
    props = Properties(PacketTypes.SUBSCRIBE)
    props.UserProperty = [("saguin-filter", one) for one in values]
    return props


_TOPIC_OF = {}


def _index(thing, count):
    return saguin.partition(_TOPIC_OF[thing], count)


def things(site, template, count, want=2):
    """Names under one site that fill every slice, with their topics."""
    found = []
    while len(found) < 60:
        thing = "t{}".format(len(found))
        topic = template.format(site=site, thing=thing)
        _TOPIC_OF[thing] = topic
        found.append(thing)
        held = {}
        for one in found:
            held.setdefault(_index(one, count), []).append(one)
        if len(held) == count and min(len(v) for v in held.values()) >= want:
            return found, held
    raise AssertionError("no spread over {} slices".format(count))


def drain(records, into, at):
    into[at] = list(records)


def drain_all(readers):
    """Every member read at once, so the test waits one timeout not N."""
    got = {}
    threads = [
        threading.Thread(target=drain, args=(records, got, at))
        for at, records in readers.items()
    ]
    for one in threads:
        one.start()
    for one in threads:
        one.join(timeout=60)
    return got


# -- the hash, against the document -----------------------------------------


def test_the_rfcs_worked_examples_are_true():
    """RFC 0003 "The hash, in full" writes four vectors out so that an
    implementation can be checked against the document rather than against
    a running broker. This is that check, and it is what makes `partition`
    usable as an oracle everywhere else in this file.

    **Both halves, because the document gives both.** An implementation
    that stops after FNV-1a agrees with the third column and with nothing
    else, which is exactly the mistake the table is written to let somebody
    find.
    """
    examples = [
        ("iot/depot/events/dev-1", 9321193355118713112,
         16961261177379703000, 1, 0),
        ("iot/water/w-7/inspect", 14147658934179859562,
         16406880391913018636, 2, 4),
        ("orders/resize/thumbnails/42", 15020795679369718730,
         10596179872049748585, 0, 1),
        ("a", 12638187200555641996, 198367012849983736, 1, 0),
    ]
    for topic, fnv, mixed, by_three, by_eight in examples:
        got = saguin.topic_hash(topic)
        assert got == fnv, "{}: FNV-1a is {}, the document says {}".format(
            topic, got, fnv
        )
        assert saguin.partition(topic, 3) == by_three, topic
        assert saguin.partition(topic, 8) == by_eight, topic
        assert mixed % 3 == by_three and mixed % 8 == by_eight, (
            "{}: the document's mixed value disagrees with its own "
            "columns".format(topic)
        )
    assert len(examples) == 4, "the RFC writes four; this checked fewer"


def test_the_mixing_step_is_applied():
    """`topic_hash` alone is not the answer to "which member holds this",
    and a `partition` that forgot the mixing step would agree with it.

    **Over many topics rather than one**, because the two agree on one
    topic in eight by chance, and a single-topic version of this passed
    over a missing mixing step on the first topic it was written with.
    """
    topics = ["iot/site-{}/events/dev-{}".format(n % 7, n) for n in range(200)]
    differ = sum(1 for one in topics
                 if saguin.partition(one, 8) != saguin.topic_hash(one) % 8)
    assert differ > len(topics) // 2, (
        "{} of {} topics land where the bare FNV-1a value puts them, so the "
        "mixing step is not being applied".format(len(topics) - differ,
                                                  len(topics))
    )


def test_a_topic_scheme_repeating_an_identifier_still_spreads():
    """**The defect the mixing step exists for.** A topic carrying an
    identifier twice cancels it out of FNV-1a's low bits, so without the
    step every one of these landed in a strict subset of the shares and
    whole members were sent nothing - 2454/0/1546/0 over four, at any
    scale. The oracle is a floor on the smallest share rather than an exact
    split, which would pin this to today's constants."""
    for count in (2, 3, 4, 6, 8, 16):
        held = [0] * count
        for n in range(4000):
            held[saguin.partition(
                "devices/dev-{n}/messages/devicebound/dev-{n}".format(n=n),
                count)] += 1
        assert min(held) >= (4000 // count) // 2, (
            "over {} shares the smallest was sent {} of 4000 where an even "
            "split is {}: {}".format(count, min(held), 4000 // count, held)
        )


@pytest.mark.parametrize("count", [0, -1, 1.5, True, "3"])
def test_partition_refuses_a_count_that_is_not_one_or_more(count):
    with pytest.raises(ValueError):
        saguin.partition("iot/a/events/b", count)


def test_the_hash_is_over_bytes_and_not_characters():
    """The RFC says the topic's UTF-8 bytes rather than its characters,
    and only a non-ASCII topic can tell the two apart: a hash written over
    code points agrees with this one on every ASCII vector above."""
    topic = "iot/caf\u00e9/events/x"
    assert len(topic.encode("utf-8")) == len(topic) + 1, (
        "this topic is meant to hold one character that is two bytes"
    )
    over_bytes = 14695981039346656037
    for byte in topic.encode("utf-8"):
        over_bytes = ((over_bytes ^ byte) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    over_characters = 14695981039346656037
    for point in topic:
        over_characters = (
            ((over_characters ^ ord(point)) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
        )
    assert over_bytes != over_characters, "the two readings agree here, so this "\
        "topic cannot tell them apart"
    assert saguin.topic_hash(topic) == over_bytes


# -- what the library refuses before sending, with no broker in it ----------


@pytest.mark.parametrize("declared,said", [
    # **An empty collection declares nothing and is served everything**,
    # which is the shape a member whose computed share came out empty by
    # mistake would send - and every such member then processes the whole
    # channel while looking healthy
    # `None` stays the way to ask for everything on purpose.
    (dict(topic_hash=[]), "empty"),
    (dict(topic_hash=()), "empty"),
    (dict(saguin_filter=[]), "empty"),
    (dict(saguin_filter=()), "empty"),
    # A pair of the wrong kind used to answer with the interpreter's
    # sentence rather than this library's.
    (dict(topic_hash=(True, False)), "pair of whole numbers"),
    (dict(topic_hash=[(2, 0), True]), "pair of whole numbers"),
    (dict(topic_hash=(1, 8)), "not below"),
    (dict(topic_hash=(3, 3)), "not below"),
    (dict(topic_hash=(3, -1)), "not below"),
    (dict(topic_hash=(0, 0)), "outside 1 to"),
    (dict(topic_hash=(2147483648, 0)), "outside 1 to"),
    (dict(topic_hash=[(3, 0), (4, 1)]), "same number of partitions"),
    (dict(topic_hash=(3,)), "pair of whole numbers"),
    (dict(topic_hash=(3, 0, 1)), "pair of whole numbers"),
    (dict(topic_hash=("3", "0")), "pair of whole numbers"),
    (dict(saguin_filter=""), "non-empty call"),
    (dict(saguin_filter=[""]), "non-empty call"),
])
def test_a_declaration_that_cannot_be_right_is_refused_here(declared, said):
    """Every one of these is refused by every saguin there will ever be,
    so refusing it at the call is a sentence rather than a `0x83` arriving
    later on a SUBACK, after the caller has a generator in hand."""
    with pytest.raises(ValueError) as refused:
        declarations(**declared)
    assert said in str(refused.value), str(refused.value)


def test_declaring_nothing_is_none_rather_than_an_empty_list():
    """The get-everything path, kept apart from the empty collection that
    is refused beside it: one is a client that has never heard of slicing,
    the other is a member that meant to name one."""
    assert declarations() == []
    assert declarations(topic_hash=None, saguin_filter=None) == []


def test_what_a_declaration_puts_on_the_packet():
    assert declarations(topic_hash=(8, 1)) == [("saguin-filter", "topic_hash(8, 1)")]
    # Repeated for a member holding two slices, which the broker reads as
    # an OR - a member covering a failed peer's share.
    assert declarations(topic_hash=[(8, 1), (8, 5)]) == [
        ("saguin-filter", "topic_hash(8, 1)"),
        ("saguin-filter", "topic_hash(8, 5)"),
    ]
    # A count of 1 is legal and degenerate: one slice holding everything.
    assert declarations(topic_hash=(1, 0)) == [("saguin-filter", "topic_hash(1, 0)")]
    assert declarations() == []
    # A call this library has not heard of goes through unchanged, so a
    # broker that has grown one does not wait for a release here.
    assert declarations(saguin_filter="header(region, emea, eq, 1)") == [
        ("saguin-filter", "header(region, emea, eq, 1)")
    ]


def test_a_slice_of_a_queue_is_refused_before_the_broker_is_asked():
    """A queue already hands each job to one worker, so a slice of one is
    a second mechanism dividing one stream. Refused here, which needs no
    connection at all."""
    with pytest.raises(ValueError) as refused:
        saguin.Client("no-slicing-a-queue").queue.fetch("tasks", topic_hash=(3, 0))
    assert "queue cannot be sliced" in str(refused.value)


def test_a_declaration_does_not_stick_to_the_callers_properties():
    """A subscriber that builds one Properties and reuses it would
    otherwise accumulate every slice it has ever declared, and be served
    the union of them."""
    from saguin.client import _declaring

    mine = Properties(PacketTypes.SUBSCRIBE)
    mine.UserProperty = [("x-app", "keep me")]
    out = _declaring(mine, declarations(topic_hash=(4, 2)))
    assert out is not mine
    assert list(mine.UserProperty) == [["x-app", "keep me"]] or \
        [tuple(p) for p in mine.UserProperty] == [("x-app", "keep me")]
    sent = [tuple(one) for one in out.UserProperty]
    assert ("saguin-filter", "topic_hash(4, 2)") in sent
    assert ("x-app", "keep me") in sent, "the caller's own property was dropped"


# -- against a running broker -----------------------------------------------


def test_members_split_an_append_channel_exactly_once_and_in_order(
    producer, broker, site
):
    """**The test the whole feature is for.** Three members covering
    0..2 of three, and both halves are needed: exactly-once across the
    group catches a gap or an overlap, and per-topic order inside a member
    is the whole reason the key is the topic rather than the offset."""
    count, per_topic = 3, 3
    names, by_slice = things(site, "iot/{site}/events/{thing}", count)
    assert len(by_slice) == count, "the topics did not fill every slice"

    members = {}
    readers = {}
    for index in range(count):
        client = saguin.Client("split-{}-{}".format(index, site), durable=True)
        client.start(*broker.address)
        members[index] = client
        readers[index] = client.append.consume(
            "events", key=[site], topic_hash=(count, index), timeout=8
        )
    try:
        for run in range(per_topic):
            for thing in names:
                producer.client.append.publish(
                    "events", key=[site, thing], value=str(run).encode()
                ).wait_for_publish(10)
        got = drain_all(readers)
    finally:
        for client in members.values():
            client.close()

    # **Counted across the group.** Every record owed, once.
    owed = sorted(
        ("iot/{}/events/{}".format(site, thing), str(run).encode())
        for thing in names for run in range(per_topic)
    )
    arrived = sorted(
        (record.topic, record.payload)
        for records in got.values() for record in records
    )
    assert arrived == owed, (
        "{} records arrived where {} were published: {} duplicated, {} "
        "missing".format(
            len(arrived), len(owed),
            sorted(set(x for x in arrived if arrived.count(x) > 1)),
            sorted(set(owed) - set(arrived)),
        )
    )

    # **And each in the member the hash says owns it.** Exactly-once alone
    # would pass if two members had swapped their shares.
    for index, records in got.items():
        for record in records:
            assert saguin.partition(record.topic, count) == index, (
                "{} landed in member {}, and the hash puts it in {}".format(
                    record.topic, index,
                    saguin.partition(record.topic, count))
            )
        assert records, "member {} was owed topics and received none".format(index)

    # **Per-topic order inside each member.**
    for index, records in got.items():
        seen = {}
        for record in records:
            seen.setdefault(record.topic, []).append(record.payload)
        for topic, payloads in seen.items():
            assert payloads == [str(n).encode() for n in range(per_topic)], (
                "{} arrived out of order at member {}: {}".format(
                    topic, index, payloads)
            )


def test_a_member_declaring_an_index_it_does_not_hold_receives_nothing(
    producer, broker, site
):
    """The other half of the split, said on its own: a slice is a
    predicate, so a member declaring one it holds no topics for is fed
    nothing rather than fed everything."""
    names, by_slice = things(site, "iot/{site}/events/{thing}", 2)
    mine = by_slice[0][0]

    listener = saguin.Client("held-{}".format(site), durable=True)
    listener.start(*broker.address)
    try:
        # One topic, and the member declares the *other* slice of two.
        wrong = 1 - saguin.partition(
            "iot/{}/events/{}".format(site, mine), 2)
        records = listener.append.consume(
            "events", key=[site, mine], topic_hash=(2, wrong), timeout=6
        )
        producer.client.append.publish(
            "events", key=[site, mine], value=b"not yours"
        ).wait_for_publish(10)
        assert list(records) == [], "a record outside the declared slice arrived"
    finally:
        listener.close()

    # **The same record, read by somebody who declared nothing.** Without
    # this the empty read above would pass just as well against a broker
    # that had sent nobody anything, which is the way this test would
    # stop measuring the declaration and never say so. A fresh session has
    # no position on this channel, so it is served the record the declared
    # member was not.
    proof = saguin.Client("undeclared-{}".format(site), durable=True)
    proof.start(*broker.address)
    try:
        got = [r.payload for r in proof.append.consume(
            "events", key=[site, mine], timeout=8)]
    finally:
        proof.close()
    assert b"not yours" in got, (
        "the record reached no reader at all, so the empty read above says "
        "nothing about the slice: {}".format(got)
    )


def test_a_subscriber_declaring_nothing_still_gets_everything(
    producer, broker, site
):
    """Declaring nothing is every client that has never heard of this, and
    it sits beside declared members without narrowing them: what a
    declaration narrows is the subscription it was made on."""
    names, by_slice = things(site, "iot/{site}/events/{thing}", 2)

    everything = saguin.Client("all-{}".format(site), durable=True)
    half = saguin.Client("half-{}".format(site), durable=True)
    everything.start(*broker.address)
    half.start(*broker.address)
    try:
        readers = {
            "all": everything.append.consume("events", key=[site], timeout=8),
            "half": half.append.consume(
                "events", key=[site], topic_hash=(2, 0), timeout=8),
        }
        for thing in names:
            producer.client.append.publish(
                "events", key=[site, thing], value=b"x"
            ).wait_for_publish(10)
        got = drain_all(readers)
    finally:
        everything.close()
        half.close()

    assert sorted(r.topic for r in got["all"]) == sorted(
        "iot/{}/events/{}".format(site, thing) for thing in names
    ), "the undeclared subscriber was narrowed by somebody else's declaration"
    assert sorted(r.topic for r in got["half"]) == sorted(
        "iot/{}/events/{}".format(site, thing)
        for thing in by_slice[0]
    )
    assert len(got["half"]) < len(got["all"]), (
        "the declared member took everything, so this measured no slice"
    )


def test_members_split_a_latest_channel_on_both_halves(producer, broker, site):
    """A `latest` channel takes the predicate on the current-state pass as
    well as on the changes. A member sent the whole of current state and
    then only its share of the changes holds a copy that starts complete
    and drifts, which reads as correct on the day it is set up and as loss
    a week later."""
    count = 2
    names, by_slice = things(site, "iot/{site}/state/{thing}", count)
    for thing in names:
        producer.client.latest.set(
            "state", key=[site, thing], value=b"before"
        ).wait_for_publish(10)

    members, readers = {}, {}
    for index in range(count):
        client = saguin.Client("state-{}-{}".format(index, site), durable=True)
        client.start(*broker.address)
        members[index] = client
        readers[index] = client.latest.consume(
            "state", key=[site], topic_hash=(count, index), timeout=8
        )
    try:
        for thing in names:
            producer.client.latest.set(
                "state", key=[site, thing], value=b"after"
            ).wait_for_publish(10)
        got = drain_all(readers)
    finally:
        for client in members.values():
            client.close()

    for half in ("catch_up", "change"):
        owed = sorted("iot/{}/state/{}".format(site, thing) for thing in names)
        arrived = sorted(
            record.topic
            for records in got.values() for record in records
            if record.is_catch_up == (half == "catch_up")
        )
        assert arrived == owed, "the {} half was not split cleanly: {}".format(
            half, arrived
        )
    for index, records in got.items():
        assert records, "member {} received neither half".format(index)
        for record in records:
            assert saguin.partition(record.topic, count) == index, record.topic


def test_a_member_can_hold_two_slices_at_once(producer, broker, site):
    """Repeating the property is an OR, which is how a member covers a
    failed peer's share."""
    count = 4
    names, by_slice = things(site, "iot/{site}/events/{thing}", count, want=1)

    listener = saguin.Client("two-slices-{}".format(site), durable=True)
    listener.start(*broker.address)
    try:
        records = listener.append.consume(
            "events", key=[site], topic_hash=[(count, 0), (count, 2)], timeout=8
        )
        for thing in names:
            producer.client.append.publish(
                "events", key=[site, thing], value=b"x"
            ).wait_for_publish(10)
        got = sorted(r.topic for r in records)
    finally:
        listener.close()

    owed = sorted(
        "iot/{}/events/{}".format(site, thing)
        for thing in by_slice[0] + by_slice[2]
    )
    assert got == owed
    assert by_slice[1] and by_slice[3], (
        "no topic fell outside the two slices, so this measured nothing"
    )


def test_broadcast_takes_a_slice_through_pahos_own_subscribe(
    producer, broker, site
):
    """Broadcast has no channel and so no verb: a topic no channel claims
    is subscribed to with paho's own call, which is why the keywords are
    on it as well as on `consume`."""
    count = 2
    names, by_slice = things(site, "shout/{site}/{thing}", count)

    arrived, granted = {}, {}
    members = {}
    for index in range(count):
        client = saguin.Client("shout-{}-{}".format(index, site))
        arrived[index] = queue.Queue()
        granted[index] = queue.Queue()
        client.on_message = (
            lambda cl, u, msg, at=index: arrived[at].put(msg.topic)
        )
        client.on_subscribe = (
            lambda cl, u, mid, codes, props, at=index: granted[at].put(codes)
        )
        client.start(*broker.address)
        client.subscribe("shout/{}/+".format(site), qos=1,
                         topic_hash=(count, index))
        codes = granted[index].get(timeout=10)
        assert codes[0].value == 1, "the broadcast subscribe was refused: {}".format(
            codes
        )
        members[index] = client
    try:
        for thing in names:
            producer.client.publish(
                "shout/{}/{}".format(site, thing), b"x"
            ).wait_for_publish(10)
        got = {}
        for index in range(count):
            held = []
            while True:
                try:
                    held.append(arrived[index].get(timeout=4))
                except queue.Empty:
                    break
            got[index] = held
    finally:
        for client in members.values():
            client.close()

    owed = sorted("shout/{}/{}".format(site, thing) for thing in names)
    assert sorted(t for held in got.values() for t in held) == owed
    for index, held in got.items():
        assert held, "broadcast member {} received nothing".format(index)
        for topic in held:
            assert saguin.partition(topic, count) == index, topic


# -- what the broker refuses, driven rather than described ------------------


def test_the_broker_refuses_a_slice_on_a_shared_subscription(reader, site):
    """A shared subscription already divides a stream between its members,
    so a slice of one is a second mechanism doing the same job. The SDK
    offers no shared subscription, so this is driven with plain paho."""
    with reader() as r:
        codes = r.subscribe(
            "$share/g/iot/{}/events/+".format(site), qos=1,
            properties=declaring("topic_hash(2, 0)"),
        )
    assert codes[0].value == NOT_AUTHORIZED, codes


def test_the_broker_refuses_a_slice_on_a_queue(reader):
    """The same rule at the other prefix, and the reason the library's own
    refusal is a convenience rather than the only thing standing between a
    caller and a wrong answer."""
    with reader() as r:
        codes = r.subscribe(
            "$saguin/queue/tasks", qos=1,
            properties=declaring("topic_hash(2, 0)"),
        )
    assert codes[0].value == NOT_AUTHORIZED, codes


def test_the_broker_refuses_arguments_the_wrong_way_round(broker, site):
    """`topic_hash(1, 8)` reaches the wire through `saguin_filter=`, which
    is the door for a call this library does not check. It proves two
    things at once: that the door reaches the packet, and that an index
    below its count is enforced at the broker rather than only here."""
    client = saguin.Client("swapped-{}".format(site), durable=True)
    client.start(*broker.address)
    try:
        with pytest.raises(saguin.SubscriptionRefused) as refused:
            client.append.consume(
                "events", key=[site], saguin_filter="topic_hash(1, 8)"
            )
    finally:
        client.close()
    codes = [code.value for _, code in refused.value.refused]
    assert codes == [NOT_AUTHORIZED], codes


def test_the_broker_refuses_a_declaration_it_cannot_read(broker, site):
    """The expression form the call syntax replaced, and a function saguin
    does not have. Both reach the wire through `saguin_filter=`."""
    client = saguin.Client("unreadable-{}".format(site), durable=True)
    client.start(*broker.address)
    try:
        for value in ("$topic_hash % 3 == 0", "TOPIC_HASH(3, 0)",
                      "nonesuch(3, 0)"):
            with pytest.raises(saguin.SubscriptionRefused) as refused:
                client.append.consume(
                    "events", key=[site], saguin_filter=value
                )
            codes = [code.value for _, code in refused.value.refused]
            assert codes == [NOT_AUTHORIZED], "{}: {}".format(value, codes)
    finally:
        client.close()


def test_a_refusal_carries_the_brokers_own_sentence(broker, site):
    """**The code is not the answer.** `0x83` spelled out is
    "Implementation specific error", which tells the person reading it
    nothing; saguin sends a sentence beside it naming the rule that was
    broken, and a client that drops it leaves its user with the code."""
    client = saguin.Client("said-{}".format(site), durable=True)
    client.start(*broker.address)
    try:
        with pytest.raises(saguin.SubscriptionRefused) as refused:
            client.append.consume(
                "events", key=[site], saguin_filter="TOPIC_HASH(3, 0)")
    finally:
        client.close()

    said = refused.value.reason_string
    assert said, (
        "the broker refused with no sentence, or the SDK dropped it: the "
        "caller is left with {}".format(refused.value)
    )
    assert "saguin-filter" in said and "topic_hash" in said, said
    # And it reaches the message the caller actually reads, not only an
    # attribute they would have to know to look for.
    assert said in str(refused.value)


def test_different_mistakes_get_different_sentences(broker, site):
    """RFC 0003: "An argument that is not a number and one that is too
    large get different sentences, because they send a reader looking in
    different places." Driven, because a broker answering every refusal
    with one sentence would satisfy the test above."""
    values = ["TOPIC_HASH(3, 0)", "topic_hash(3, 3)",
              "topic_hash(10000000000000000000000, 0)", "topic_hash(abc, 0)"]
    said = set()
    for n, value in enumerate(values):
        client = saguin.Client("apart-{}-{}".format(n, site), durable=True)
        client.start(*broker.address)
        try:
            with pytest.raises(saguin.SubscriptionRefused) as refused:
                client.append.consume("events", key=[site], saguin_filter=value)
        finally:
            client.close()
        said.add(refused.value.reason_string)
    assert len(said) == len(values), (
        "{} different mistakes produced {} sentences: {}".format(
            len(values), len(said), said)
    )
