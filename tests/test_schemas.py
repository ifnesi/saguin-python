"""Serializing a payload against a schema, and reading one back.

What saguin promises, and what these measure against.  A schema registry
is a `latest` channel and a convention: the schema text is published to a
topic in it, retired with a zero-length payload, and a producer names
**that topic** - not an id - in a User Property called `schema`.  A
consumer reads the property, point-reads the topic, and caches the answer.
The pointer is a whole topic because a bare name lets two publishers in
different domains choose the same one and lets the second silently replace
the first.  And a consumer follows a pointer **only where it lands inside
the registry's own filter**, since an ACL governs who may write a topic
and never who may name one.
"""

import json

import pytest

import saguin
from saguin import schemas

AVRO = json.dumps({
    "type": "record",
    "name": "Reading",
    "fields": [{"name": "site", "type": "string"},
               {"name": "temp", "type": "double"}],
})

PROTO = """syntax = "proto3";
message Reading {
  string site = 1;
  double temp = 2;
}
"""


def register(client, topic, text):
    key = topic.split("/", 1)[1]
    client.latest.set("schemas", key=[key], value=text.encode()).wait_for_publish(10)


# -- the matcher, with no broker in it --------------------------------------


def test_a_pointer_is_followed_only_inside_the_registry():
    """The one place this library matches rather than composes, and the
    reason is a leak: a publisher may name any topic, including a `latest`
    channel holding device state."""
    assert schemas.inside("schemas/#", "schemas/acme/weather/v1")
    assert schemas.inside("schemas/#", "schemas")
    assert not schemas.inside("schemas/#", "state/site42/temp")
    assert schemas.inside("iot/+/schemas/+", "iot/acme/schemas/v1")
    assert not schemas.inside("iot/+/schemas/+", "iot/acme/schemas/v1/deeper")
    # Braces are expanded first, since that is what the broker matches on.
    assert schemas.inside("schemas/{acme,other}/#", "schemas/acme/v1")
    assert not schemas.inside("schemas/{acme,other}/#", "schemas/third/v1")


def test_the_format_is_read_off_the_schema_when_nobody_said():
    """A `.proto` can only be read by protobuf and an avro schema is JSON,
    so the schema settles the question a missing Content Type would have
    answered. It is not a guess about the payload."""
    assert schemas.format_of(PROTO) in schemas.PROTOBUF_TYPES
    assert schemas.format_of(AVRO) in schemas.AVRO_TYPES
    assert schemas.format_of("neither one nor the other") is None


# -- against a running broker -----------------------------------------------


@pytest.mark.parametrize("kind,text", [("avro", AVRO), ("protobuf", PROTO)])
def test_a_payload_goes_out_encoded_and_comes_back_decoded(
    producer, broker, site, kind, text
):
    topic = "schemas/{}/{}".format(site, kind)
    register(producer.client, topic, text)

    with saguin.Client("schema-reader-" + site, durable=True,
                       schema_registry="schemas") as c:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], timeout=10)

        producer.client.append.publish(
            "events", key=[site, kind],
            value={"site": site, "temp": 21.5},
            schema=topic,
        ).wait_for_publish(10)

        record = next(records)

        # **Encoded, and not merely bytes.** `isinstance(payload, bytes)`
        # is true of a payload the library never encoded at all, so it
        # proves nothing about the writing half - what does is that the
        # bytes are neither the JSON of the value nor as long as it.
        as_json = json.dumps({"site": site, "temp": 21.5}).encode()
        assert isinstance(record.payload, bytes)
        assert record.payload != as_json
        assert len(record.payload) < len(as_json), (
            "the payload is no smaller than its JSON, so it may not be encoded "
            "at all: {!r}".format(record.payload)
        )
        assert site.encode() in record.payload, "the string field should be in there"

        # The pointer travels as an ordinary header, so a consumer with no
        # schema support still sees where to look.
        assert record.schema == topic
        assert record.headers["schema"] == topic

        # And the publisher said how it wrote them, rather than leaving it
        # to be worked out.
        assert record.properties.ContentType in schemas.SCHEMA_TYPES

        # **Decoded while the client is still connected**, because a
        # schema is fetched from the broker rather than carried on the
        # record: reading one is a question, and a question needs a
        # connection for its answer to arrive on.
        read = record.deserialized
    assert read["site"] == site
    assert float(read["temp"]) == 21.5


def test_decoding_after_the_client_closed_says_what_is_wrong(
    producer, broker, site
):
    """`record.deserialized` reaches for the schema, so it needs the connection
    the record was read on - and saying "the broker did not acknowledge
    within 10s" about a closed client sends the reader hunting the wrong
    thing."""
    topic = "schemas/{}/closed".format(site)
    register(producer.client, topic, AVRO)

    with saguin.Client("closer-" + site, durable=True,
                       schema_registry="schemas") as c:
        c.start(*broker.address)
        records = c.append.consume("events", key=[site], timeout=10)
        producer.client.append.publish(
            "events", key=[site, "x"], value={"site": site, "temp": 1.0},
            schema=topic,
        ).wait_for_publish(10)
        record = next(records)

    with pytest.raises(RuntimeError) as refused:
        record.deserialized
    assert "not connected" in str(refused.value)


def test_a_pointer_outside_the_registry_is_refused(producer, broker, site):
    """The rule that makes following somebody else's pointer safe."""
    with saguin.Client("nosy-" + site, durable=True,
                       schema_registry="schemas") as c:
        c.start(*broker.address)
        with pytest.raises(schemas.SchemaError) as refused:
            c.schema_text("iot/{}/state/temp".format(site))
    said = str(refused.value)
    assert "outside" in said and "schemas/#" in said


def test_a_client_that_was_not_told_where_schemas_live_says_so(broker, site):
    with saguin.Client("untold-" + site, durable=True) as c:
        c.start(*broker.address)
        with pytest.raises(schemas.SchemaError) as refused:
            c.schema_text("schemas/anything")
    assert "schema_registry" in str(refused.value)


def test_an_unregistered_schema_is_refused_before_anything_is_sent(
    producer, broker, site
):
    """Which is the whole reason a schema must be registered first: a
    record written against a schema nobody can fetch is a record nobody
    can read."""
    with pytest.raises(schemas.SchemaError) as refused:
        producer.client.append.publish(
            "events", key=[site, "x"], value={"site": site, "temp": 1.0},
            schema="schemas/{}/never-registered".format(site),
        )
    assert "no schema is registered" in str(refused.value)


def test_a_payload_that_does_not_fit_the_schema_is_refused(producer, site):
    topic = "schemas/{}/fit".format(site)
    register(producer.client, topic, AVRO)
    with pytest.raises(schemas.SchemaError) as refused:
        producer.client.append.publish(
            "events", key=[site, "x"], value={"site": site},  # temp missing
            schema=topic,
        )
    assert "does not fit" in str(refused.value)


def test_a_record_naming_no_schema_says_so(producer, reader, site):
    with reader() as r:
        r.subscribe("iot/{}/events/+".format(site))
        producer.client.append.publish("events", key=[site, "plain"], value=b"bytes")
        record = saguin.Message(r.next())
    assert record.schema is None
    with pytest.raises(schemas.SchemaError) as refused:
        record.deserialized
    assert "names no schema" in str(refused.value)


def test_a_schema_is_remembered_until_it_is_forgotten(producer, broker, site):
    """**The test that was wrong before it was right.** It used to be
    called "a republished schema is read again" and passed only because it
    asked a *fresh* client, which has nothing cached - so it asserted
    nothing about the cache and a mutation to the cache survived it.

    What the code actually does: a schema is kept for the life of the
    connection, and republishing at the same topic is not noticed. That is
    a limit rather than an oversight - Avro's schemaless decoding cannot
    reliably tell that it has the wrong schema, so there is no failure to
    invalidate on. It costs nothing where the convention is followed,
    because the topic is the identity and a new version is a new topic.
    """
    topic = "schemas/{}/evolving".format(site)
    register(producer.client, topic, AVRO)

    wider = json.dumps({
        "type": "record", "name": "Reading",
        "fields": [{"name": "site", "type": "string"},
                   {"name": "temp", "type": "double"},
                   {"name": "unit", "type": "string", "default": "C"}],
    })

    with saguin.Client("evolver-" + site, durable=True,
                       schema_registry="schemas") as c:
        c.start(*broker.address)
        assert json.loads(c.schema_text(topic)) == json.loads(AVRO)

        register(producer.client, topic, wider)

        # Still the first, on the same client: this is the cache, and it
        # is what the mutation that survived was about.
        assert json.loads(c.schema_text(topic)) == json.loads(AVRO), (
            "a schema was re-read from the broker when it should have been "
            "remembered"
        )

        # Until it is dropped, which is the way out for anybody who does
        # republish at the same topic.
        c.forget_schema(topic)
        assert json.loads(c.schema_text(topic)) == json.loads(wider)

        # And forgetting the lot works the same way.
        register(producer.client, topic, AVRO)
        c.forget_schema()
        assert json.loads(c.schema_text(topic)) == json.loads(AVRO)


def test_a_broadcast_record_is_decoded_through_the_client_that_read_it(
    producer, broker, site
):
    """**The one path this library does not hand you a record on.** A
    broadcast topic is ordinary MQTT, read through paho's own `subscribe`
    and `on_message`, so what arrives is paho's message - `client.record`
    is what turns it into one of these, and the client is what fetches the
    schema."""
    import queue as q
    import time

    topic = "schemas/{}/broadcast".format(site)
    register(producer.client, topic, AVRO)

    arrived = q.Queue()
    with saguin.Client("shouter-" + site, schema_registry="schemas") as c:
        c.start(*broker.address)
        c.on_message = lambda cl, u, msg: arrived.put(msg)
        c.subscribe("broadcast/{}/#".format(site), qos=1)
        time.sleep(0.4)

        producer.client.publish(
            "broadcast/{}/hello".format(site),
            {"site": site, "temp": 21.5},
            schema=topic,
        ).wait_for_publish(10)

        record = c.record(arrived.get(timeout=10))
        assert record.schema == topic
        assert record.deserialized == {"site": site, "temp": 21.5}
        # It landed in broadcast rather than in a channel, which is the
        # point of the path: no offset, because no channel holds it.
        assert record.offset is None


def test_a_registry_that_is_not_a_latest_channel_says_so(broker, site):
    """Pointing it at the wrong kind of channel fails later and
    confusingly otherwise: an append channel would take the writes and
    answer no read."""
    with saguin.Client("badreg-" + site, schema_registry="events") as c:
        c.start(*broker.address)
        with pytest.raises(schemas.SchemaError) as refused:
            c.schema_text("iot/anything")
    said = str(refused.value)
    assert "append channel" in said and "latest" in said


@pytest.mark.parametrize("write", ["latest", "queue"])
def test_every_write_path_takes_a_schema(producer, broker, site, write):
    """append is covered above; these are the other two, so that "it works
    everywhere" is measured rather than asserted."""
    topic = "schemas/{}/{}".format(site, write)
    register(producer.client, topic, AVRO)
    value = {"site": site, "temp": 21.5}

    with saguin.Client("{}-reader-{}".format(write, site), durable=True,
                       schema_registry="schemas") as c:
        c.start(*broker.address)
        if write == "latest":
            records = c.latest.consume("state", key=[site], timeout=10)
            producer.client.latest.set(
                "state", key=[site, "temp"], value=value, schema=topic
            ).wait_for_publish(10)
        else:
            records = c.queue.fetch("retried", timeout=10)
            producer.client.queue.publish(
                "retried", key=[site, "1"], value=value, schema=topic
            ).wait_for_publish(10)

        record = next(records)
        assert record.schema == topic
        assert record.deserialized == value
        if write == "queue":
            c.queue.ack(record)
