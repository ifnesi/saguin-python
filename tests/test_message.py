"""What the broker stamps on a delivery, and what the SDK makes of it.

What saguin promises, and what these measure against.  Every record has
an id, an offset the broker assigns and a receipt time.  User Properties
come back in the order the publisher wrote them, a name that appears more
than once included.  The `saguin-` prefix is reserved: a client's is
stripped, so that a publisher cannot forge metadata a consumer would read
as the broker's.  `saguin-channel` is on a delivery only where the
consumer's filter reaches more than one channel, and a queue offer never
carries it because a worker names one queue.  A `latest` subscriber is
sent the current value of every topic its filter reaches with the RETAIN
flag set, and every change after that without it.
"""

from datetime import datetime, timezone

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import saguin


def a_publish_with(pairs):
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = list(pairs)
    return props


def test_a_delivery_carries_the_records_position_and_receipt_time(
    producer, reader, site
):
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        producer.publish("iot/{}/events/one".format(site), b"1")
        producer.publish("iot/{}/events/two".format(site), b"2")

        first = saguin.Message(r.next())
        second = saguin.Message(r.next())

        assert isinstance(first.offset, int)
        assert second.offset > first.offset

        now = datetime.now(timezone.utc)
        assert first.timestamp.tzinfo is not None
        assert abs((now - first.timestamp).total_seconds()) < 120


def test_the_reserved_prefix_is_stripped_from_a_publishers_headers(
    producer, reader, site
):
    """A publisher cannot forge dead-letter metadata a consumer would read
    as the broker's."""
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        producer.publish(
            "iot/{}/events/thing".format(site),
            b"x",
            properties=a_publish_with(
                [("saguin-dlq-channel", "forged"), ("device", "a")]
            ),
        )
        msg = saguin.Message(r.next())

        arrived = [k for k, _ in saguin.message._user_properties(msg.properties)]
        assert "saguin-dlq-channel" not in arrived
        assert msg.headers.pairs == (("device", "a"),)


def test_headers_keep_their_order_and_their_duplicates(producer, reader, site):
    sent = [("tag", "a"), ("unit", "C"), ("tag", "b")]
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        producer.publish(
            "iot/{}/events/thing".format(site), b"x", properties=a_publish_with(sent)
        )
        headers = saguin.Message(r.next()).headers

        assert headers.pairs == tuple(sent)
        assert headers.all("tag") == ("a", "b")
        assert headers["tag"] == "a"
        assert headers.get("missing") is None
        assert dict(headers) == {"tag": "b", "unit": "C"}


def test_the_channel_name_is_there_only_where_a_filter_reaches_two(
    producer, reader, site
):
    topic = "iot/{}/events/thing".format(site)

    with reader() as one, reader() as two:
        one.subscribe("iot/{}/events/+".format(site))  # events, and nothing else
        two.subscribe("iot/{}/#".format(site))  # events and state
        producer.publish(topic, b"x")

        assert saguin.Message(one.next()).channel is None
        assert saguin.Message(two.next()).channel == "events"


def test_a_latest_value_is_catch_up_on_subscribe_and_a_change_is_not(
    producer, reader, site
):
    topic = "iot/{}/state/temperature".format(site)
    producer.publish(topic, b"18")

    with reader() as r:
        r.subscribe("iot/{}/state/+".format(site))

        state = saguin.Message(r.next())
        assert state.payload == b"18"
        assert state.is_catch_up is True

        producer.publish(topic, b"19")
        change = saguin.Message(r.next())
        assert change.payload == b"19"
        assert change.is_catch_up is False
        assert change.offset > state.offset


def test_a_queue_offer_carries_its_attempt_and_no_channel_name(
    producer, reader, site
):
    """A worker names one queue - the only subscription form a queue
    admits - so a queue offer is never ambiguous about its channel."""
    with reader() as worker:
        granted = worker.subscribe("$saguin/queue/tasks", qos=1)
        assert granted[0].value == 1, granted

        producer.publish("work/{}/jobs/1".format(site), b"do it")
        job = saguin.Message(worker.next())

        assert job.payload == b"do it"
        assert job.attempt == 1
        assert job.channel is None
        assert job.id is not None
        assert isinstance(job.offset, int)


# -- decoding, with no broker in it -----------------------------------------


class FakeMessage:
    """Only what Message reads: this is about the decoding, not the wire."""

    def __init__(self, properties=None, retain=False):
        self.topic = "t"
        self.payload = b""
        self.qos = 1
        self.retain = retain
        self.dup = False
        self.mid = 1
        self.properties = properties


def test_a_delivery_with_no_properties_answers_none_rather_than_raising():
    """What a 3.1.1 subscriber receives: the topic and the payload, and
    none of the rest."""
    msg = saguin.Message(FakeMessage(properties=None))
    assert msg.id is None
    assert msg.offset is None
    assert msg.timestamp is None
    assert msg.channel is None
    assert msg.attempt is None
    assert len(msg.headers) == 0


def test_one_user_property_is_read_the_same_as_several():
    """A caller may write a lone User Property as a bare pair, which is
    the spelling paho's own documentation shows.  paho wraps it into a
    list on assignment, so both spellings reach the decoder as one
    shape - asserted here rather than assumed, because the decoder is
    written on the strength of it."""
    one = Properties(PacketTypes.PUBLISH)
    one.UserProperty = ("device", "a")
    assert one.UserProperty == [("device", "a")]
    assert saguin.Message(FakeMessage(one)).headers.pairs == (("device", "a"),)

    several = Properties(PacketTypes.PUBLISH)
    several.UserProperty = [("device", "a"), ("device", "b")]
    assert saguin.Message(FakeMessage(several)).headers.all("device") == ("a", "b")


def test_headers_is_a_sequence_rather_than_a_mapping():
    headers = saguin.Headers([("tag", "a"), ("tag", "b")])
    assert len(headers) == 2
    assert "tag" in headers
    assert "other" not in headers
    assert list(headers) == [("tag", "a"), ("tag", "b")]
    assert headers.names() == ("tag", "tag")


def test_a_timestamp_is_read_as_utc_milliseconds():
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = [("saguin-timestamp", "1756900000123")]
    when = saguin.Message(FakeMessage(props)).timestamp
    assert when == datetime(2025, 9, 3, 11, 46, 40, 123000, tzinfo=timezone.utc)
    assert when.tzinfo is timezone.utc
