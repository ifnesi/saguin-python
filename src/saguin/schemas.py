"""Serializing a payload against a schema, and reading one back.

**A schema registry needs nothing from the broker.** A `latest` channel is
already a key-value store with delete, readable one key at a time, so a
registry is a channel and a convention: register by publishing the schema
text to a topic in it, retire with a zero-length payload, produce with a
User Property named ``schema`` carrying **the schema's topic**, and
consume by reading that property and point-reading the topic it names.

**The pointer is a whole topic rather than a bare id**, which is what
makes the convention work without an allocator: a bare name lets two
publishers in different domains pick the same one and lets the second
silently replace the first, and says nothing about where to look it up.

Most of the reading half here is carried over from saguin-viewer's
deserializer, which had already met the parts that bite - the several spellings
of each Content Type, a publisher that names a schema and no Content Type,
compiling protobuf at runtime without two schemas colliding in one
descriptor pool, and choosing between the messages a `.proto` defines.
Two implementations of one convention drift; this is the same one.
"""

import hashlib
import json
import threading

from .channels import inside  # noqa: F401  (re-exported: the registry check)

PROTOBUF_TYPES = ("application/x-protobuf", "application/protobuf",
                  "application/vnd.google.protobuf")
AVRO_TYPES = ("application/avro", "application/x-avro", "avro/binary",
              "application/vnd.apache.avro+binary")
SCHEMA_TYPES = PROTOBUF_TYPES + AVRO_TYPES

# The User Property that points at a schema. Not `saguin-schema`, which is
# the name that looks most official and is the one that would not survive:
# the reserved prefix is stripped from anything a client sends.
SCHEMA_PROPERTY = "schema"

_lock = threading.Lock()
_compiled = {}


class SchemaError(Exception):
    """A schema could not be found, read, or used on these bytes."""


def format_of(text):
    """Which reader a schema needs, read off the schema itself.

    A fallback for a publisher that names a schema and no Content Type,
    which is the common shape in the wild. It is not a guess about the
    payload: a `.proto` can only be read by protobuf and an Avro schema is
    JSON, so the schema settles the question the missing header would have
    answered. Where a header is present it wins - a publisher saying what
    it sent is better evidence than anything inferred about it.
    """
    head = text.lstrip()[:400]
    if head.startswith("syntax") or "message " in head or "package " in head:
        return PROTOBUF_TYPES[0]
    try:
        doc = json.loads(text)
    except ValueError:
        return None
    return AVRO_TYPES[0] if isinstance(doc, (dict, list)) else None


def compiled_message(topic, text):
    """The message class a schema describes, compiled and cached.

    **Keyed by the schema's topic and a digest of its text**, so a schema
    republished at the same topic is compiled again rather than served
    from the last one.

    **Each schema gets a descriptor pool of its own**, which is not
    tidiness: protobuf's default pool refuses a second file registered
    under the same name, so generating a module for a republished schema
    fails against the pool rather than against the schema.
    """
    digest = hashlib.sha256(text.encode()).hexdigest()
    with _lock:
        hit = _compiled.get(topic)
        if hit and hit[0] == digest:
            return hit[1]

    try:
        from grpc_tools import protoc
    except ImportError:
        raise SchemaError(
            "reading a protobuf payload needs `grpcio-tools`, which is not "
            "installed - in your virtual environment, pip install 'saguin[protobuf]'"
        )
    import os
    import shutil
    import tempfile

    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    work = tempfile.mkdtemp(prefix="saguin-schema-")
    try:
        proto = os.path.join(work, "schema.proto")
        out = os.path.join(work, "schema.pb")
        with open(proto, "w") as f:
            f.write(text)
        if protoc.main(["protoc", "-I", work, "--descriptor_set_out=" + out, proto]):
            raise SchemaError("the schema at {!r} is not valid proto3".format(topic))
        with open(out, "rb") as f:
            blob = f.read()
    finally:
        shutil.rmtree(work, ignore_errors=True)

    pool = descriptor_pool.DescriptorPool()
    files = descriptor_pb2.FileDescriptorSet()
    files.ParseFromString(blob)
    names = []
    for one in files.file:
        pool.Add(one)
        prefix = one.package + "." if one.package else ""
        names.extend(prefix + m.name for m in one.message_type)
    if not names:
        raise SchemaError("the schema at {!r} defines no message".format(topic))

    # **One message per schema topic is the convention.** The pointer is a
    # whole topic, so a schema holds the one thing that topic names. Where
    # a file holds several, the one whose name matches the topic wins and
    # the rest are listed rather than guessed between.
    chosen = names[0] if len(names) == 1 else None
    if chosen is None:
        want = [p.lower().replace("_", "") for p in topic.rstrip("/").split("/")[-2:]]
        for n in names:
            if n.rsplit(".", 1)[-1].lower().replace("_", "") in want:
                chosen = n
                break
    if chosen is None:
        raise SchemaError(
            "the schema at {!r} defines {} and nothing says which describes this "
            "message - the convention is one message per schema topic".format(
                topic, ", ".join(sorted(names))
            )
        )

    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName(chosen))
    with _lock:
        if len(_compiled) > 200:
            _compiled.clear()
        _compiled[topic] = (digest, cls)
    return cls


def _avro_schema(topic, text):
    try:
        import fastavro
    except ImportError:
        raise SchemaError(
            "reading an avro payload needs `fastavro`, which is not installed - "
            "in your virtual environment, pip install 'saguin[avro]'"
        )
    try:
        return fastavro, fastavro.parse_schema(json.loads(text))
    except Exception as e:  # noqa: BLE001 - the schema's fault, said plainly
        raise SchemaError(
            "the schema at {!r} is not valid avro: {}: {}".format(
                topic, type(e).__name__, e
            )
        )


def reader_for(content_type, topic, text):
    """The Content Type to use, and whether it was inferred rather than sent.

    The publisher's wins where there is one; otherwise the schema itself
    says which reader it needs. Those are different claims and the caller
    is told which it got.
    """
    if content_type and content_type.lower().split(";")[0] in SCHEMA_TYPES:
        return content_type.lower().split(";")[0], False
    inferred = format_of(text)
    if inferred is None:
        raise SchemaError(
            "no Content Type on the message, and the schema at {!r} is neither "
            "proto3 nor an avro schema - so nothing says how to read these "
            "bytes".format(topic)
        )
    return inferred, True


def serialize(value, topic, text, content_type=None):
    """Serialize ``value`` against the schema at ``topic``.

    Answers the bytes and the Content Type they were written with, so a
    publisher can say what it sent rather than leaving a consumer to work
    it out.
    """
    ctype, _ = reader_for(content_type, topic, text)
    if ctype in PROTOBUF_TYPES:
        cls = compiled_message(topic, text)
        message = cls()
        if isinstance(value, cls):
            message = value
        else:
            from google.protobuf.json_format import ParseDict

            try:
                ParseDict(value, message)
            except Exception as e:  # noqa: BLE001
                raise SchemaError(
                    "this does not fit {}: {}: {}".format(
                        cls.DESCRIPTOR.name, type(e).__name__, e
                    )
                )
        return message.SerializeToString(), ctype

    fastavro, schema = _avro_schema(topic, text)
    import io

    out = io.BytesIO()
    try:
        fastavro.schemaless_writer(out, schema, value)
    except Exception as e:  # noqa: BLE001
        raise SchemaError(
            "this does not fit the avro schema at {!r}: {}: {}".format(
                topic, type(e).__name__, e
            )
        )
    return out.getvalue(), ctype


def deserialize(raw, topic, text, content_type=None):
    """One payload, read through the schema its headers name."""
    ctype, _ = reader_for(content_type, topic, text)
    if ctype in PROTOBUF_TYPES:
        from google.protobuf.json_format import MessageToDict

        cls = compiled_message(topic, text)
        message = cls()
        try:
            message.ParseFromString(raw)
        except Exception as e:  # noqa: BLE001 - shown, not hidden
            raise SchemaError(
                "these bytes are not {}: {}: {}".format(
                    cls.DESCRIPTOR.name, type(e).__name__, e
                )
            )
        return MessageToDict(message, preserving_proto_field_name=True,
                             always_print_fields_with_no_presence=True)

    fastavro, schema = _avro_schema(topic, text)
    import io

    try:
        return fastavro.schemaless_reader(io.BytesIO(raw), schema)
    except Exception as e:  # noqa: BLE001
        raise SchemaError(
            "these bytes do not fit the avro schema at {!r}: {}: {}".format(
                topic, type(e).__name__, e
            )
        )
