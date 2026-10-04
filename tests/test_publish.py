"""What goes on the wire when the SDK publishes.

What saguin promises, and what these measure against: a producer may
supply a UUIDv7 as the User Property ``saguin-id``, and the broker stores
it as the record's Message ID - unchanged by redelivery, dead-lettering
and replay, so that a consumer can always deduplicate on it.  A publisher
that supplies none is given one the broker generates.

Every assertion below is made against a message read back by plain paho.
"""

import uuid

import pytest
from paho.mqtt import client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import saguin


def raw_properties(msg):
    """The User Properties as paho handed them over - the oracle side, so
    that a decoding defect in the SDK cannot hide one on the wire."""
    found = getattr(msg.properties, "UserProperty", None) or []
    if isinstance(found, tuple) and len(found) == 2 and isinstance(found[0], str):
        found = [found]
    return [tuple(one) for one in found]


def test_the_broker_admits_an_sdk_client(producer, broker):
    assert producer.reason_code.value == 0, producer.reason_code
    broker.check_it_stayed_up()


def test_a_publish_carries_a_generated_uuidv7_message_id(producer, reader, site):
    topic = "iot/{}/events/thing".format(site)
    with reader() as r:
        assert r.subscribe("iot/{}/events/+".format(site))[0].value in (0, 1)
        info = producer.publish(topic, b"hello")

        minted = uuid.UUID(info.saguin_id)
        assert minted.version == 7
        assert minted.variant == uuid.RFC_4122

        msg = saguin.Message(r.next())
        assert ("saguin-id", info.saguin_id) in raw_properties(msg.paho)
        assert msg.id == info.saguin_id
        assert msg.payload == b"hello"
        assert msg.topic == topic


def test_a_supplied_message_id_is_the_one_stored(producer, reader, site):
    mine = saguin.new_message_id()
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        info = producer.publish(
            "iot/{}/events/thing".format(site), b"x", saguin_id=mine
        )
        assert info.saguin_id == mine
        assert saguin.Message(r.next()).id == mine


def test_a_message_id_written_into_properties_is_kept(producer, reader, site):
    mine = saguin.new_message_id()
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = [("saguin-id", mine), ("device", "a")]
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        info = producer.publish("iot/{}/events/thing".format(site), b"x", properties=props)
        assert info.saguin_id == mine
        msg = saguin.Message(r.next())
        assert msg.id == mine
        assert msg.headers["device"] == "a"


def test_the_saguin_id_argument_wins_over_one_in_properties(producer, reader, site):
    written, asked = saguin.new_message_id(), saguin.new_message_id()
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = [("saguin-id", written)]
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        info = producer.publish(
            "iot/{}/events/thing".format(site), b"x", properties=props, saguin_id=asked
        )
        assert info.saguin_id == asked
        msg = saguin.Message(r.next())
        assert msg.id == asked
        assert [v for k, v in raw_properties(msg.paho) if k == "saguin-id"] == [asked]


def test_the_callers_properties_are_neither_changed_nor_reused(producer, reader, site):
    """A publisher that builds one Properties and reuses it would
    otherwise send the first record's id on every record after it, and a
    consumer deduplicating on the id would drop the lot."""
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = [("device", "a")]

    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        first = producer.publish("iot/{}/events/one".format(site), b"1", properties=props)
        second = producer.publish("iot/{}/events/two".format(site), b"2", properties=props)

        assert first.saguin_id != second.saguin_id
        assert props.UserProperty == [("device", "a")]

        seen = {}
        for _ in range(2):
            msg = saguin.Message(r.next())
            seen[msg.topic] = msg
        assert seen["iot/{}/events/one".format(site)].id == first.saguin_id
        assert seen["iot/{}/events/two".format(site)].id == second.saguin_id
        for msg in seen.values():
            assert msg.headers.pairs == (("device", "a"),)


def test_a_publish_is_qos_1_unless_the_caller_says_otherwise(producer, reader, site):
    """Driven over broadcast, where saguin is an ordinary MQTT broker and
    a delivery carries the lower of the publish and the subscription - so
    what arrives is proof of what the client sent."""
    topic = "broadcast/{}/reading".format(site)
    with reader() as r:
        r.subscribe(topic, qos=1)

        producer.publish(topic, b"default")
        assert r.next().qos == 1

        producer.publish(topic, b"asked for none", qos=0)
        assert r.next().qos == 0


def test_a_protocol_that_is_not_mqtt_5_is_refused():
    with pytest.raises(ValueError) as refused:
        saguin.Client("someone", protocol=mqtt.MQTTv311)
    assert "MQTT 5" in str(refused.value)


def test_new_message_id_is_a_uuidv7_that_sorts_by_when_it_was_minted():
    minted = [saguin.new_message_id() for _ in range(50)]
    for one in minted:
        parsed = uuid.UUID(one)
        assert parsed.version == 7
        assert parsed.variant == uuid.RFC_4122
    assert len(set(minted)) == len(minted)
    # The first 48 bits are Unix milliseconds, so ids minted in order do
    # not sort out of it.  Ties inside one millisecond are permitted.
    stamps = [uuid.UUID(one).int >> 80 for one in minted]
    assert stamps == sorted(stamps)


def test_a_lone_message_id_written_as_paho_writes_one(producer, reader, site):
    """``props.UserProperty = (name, value)`` is the spelling paho's own
    documentation shows for a lone property, and it has to reach the
    broker as the record's id like any other."""
    mine = saguin.new_message_id()
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = ("saguin-id", mine)

    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        info = producer.publish(
            "iot/{}/events/thing".format(site), b"x", properties=props
        )
        assert info.saguin_id == mine
        assert saguin.Message(r.next()).id == mine
