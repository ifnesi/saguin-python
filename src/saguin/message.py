"""What the broker says about a record, read off the delivery.

Every field here arrives as an MQTT 5 User Property carrying a decimal
string, because that is what crosses a wire that has no types.  Turning
them into an int, a datetime and a list of the publisher's own headers is
the whole of this module, and it is this library's largest daily win:
without it every consumer writes the same four lookups, and each one of
them decides on its own what an absent property means.
"""

from datetime import datetime, timedelta, timezone

# The broker stamps every delivery with the record's identity, position
# and receipt time, adds the channel where a consumer's filter reaches
# more than one, and adds the attempt on a queue offer.  It strips
# anything else a client sent under this prefix, so what arrives under it
# is the broker's own word rather than a publisher's claim about it.
RESERVED_PREFIX = "saguin-"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _with_an_offset(raw):
    """An RFC 3339 time with its trailing ``Z`` written as the offset that
    means the same thing.

    ``fromisoformat`` did not read a ``Z`` until Python 3.11, and the
    broker writes one, so on 3.9 and 3.10 every dead-letter time would
    otherwise be unreadable.  It is a named step rather than an
    expression because on a newer interpreter the whole thing is
    invisible: ``fromisoformat`` accepts both spellings, and removing
    this would break nothing anybody here could run.  What is asserted is
    therefore the translation itself.
    """
    return raw[:-1] + "+00:00" if raw.endswith("Z") else raw


def _a_moment(raw):
    """One of the broker's RFC 3339 times, as an aware datetime.

    A value this cannot read is answered as None rather than raised over:
    a delivery is not the place to discover that two libraries disagree
    about a date format.
    """
    if not raw:
        return None
    try:
        return datetime.fromisoformat(_with_an_offset(raw))
    except ValueError:
        return None


class DeadLetter:
    """Why a record is in a dead-letter channel rather than its queue.

    The broker writes these when it moves a record, and a client cannot
    forge them: everything a publisher sends under the reserved prefix is
    stripped, so a consumer reading them is reading the broker's own word
    about work that failed.
    """

    __slots__ = ("channel", "offset", "attempts", "reason", "at", "first", "last")

    def __init__(self, channel, offset, attempts, reason, at, first, last):
        self.channel = channel
        """The queue it came from - not the channel it is in now."""
        self.offset = offset
        """Its offset in that queue."""
        self.attempts = attempts
        """How many attempts were made."""
        self.reason = reason
        """The broker's own word: ``attempts_exhausted`` or ``expired``."""
        self.at = at
        """When it was dead-lettered."""
        self.first = first
        """When it was first handed to a worker, or None if it never was."""
        self.last = last
        """When it was last handed to a worker, or None."""

    def __repr__(self):
        return "DeadLetter(channel={!r}, reason={!r}, attempts={!r})".format(
            self.channel, self.reason, self.attempts
        )


def _user_properties(properties):
    """Every User Property on a delivery, in order, duplicates kept.

    Always a list from paho, even for exactly one: ``UserProperty`` is a
    property MQTT 5 permits more than once, and paho wraps a lone pair
    into a list both when a caller assigns one and when it decodes one
    off the wire.  So a caller writing ``props.UserProperty = ("k", "v")``
    - the spelling paho's own documentation shows - needs nothing special
    here, and the publish path reads this same function.
    """
    if properties is None:
        return ()
    found = getattr(properties, "UserProperty", None)
    if not found:
        return ()
    return tuple(tuple(one) for one in found)


class Headers:
    """The publisher's own User Properties, in the order they were sent.

    Not a dict, and that is the point.  MQTT 5 permits a repeated name and
    it is the standard way to carry a list; the broker forwards the
    properties unaltered and in order, so a record keeps them as a
    sequence.  Flattening them into a mapping here would throw away what
    the publisher sent and nothing would ever say so.

    ``h["name"]`` answers the first value, ``h.all("name")`` answers every
    one of them, and ``dict(h)`` is there for whoever knows their own
    names are unique.
    """

    __slots__ = ("_pairs",)

    def __init__(self, pairs=()):
        self._pairs = tuple((str(k), v) for k, v in pairs)

    @property
    def pairs(self):
        """Every (name, value) pair, in order, duplicates kept."""
        return self._pairs

    def names(self):
        """Every name, in order, a repeated one appearing each time."""
        return tuple(k for k, _ in self._pairs)

    def all(self, name):
        """Every value sent under this name, in order.  Empty if none."""
        return tuple(v for k, v in self._pairs if k == name)

    def get(self, name, default=None):
        for k, v in self._pairs:
            if k == name:
                return v
        return default

    def __getitem__(self, name):
        for k, v in self._pairs:
            if k == name:
                return v
        raise KeyError(name)

    def __contains__(self, name):
        return any(k == name for k, _ in self._pairs)

    def __iter__(self):
        """Iterates pairs rather than names, so ``dict(headers)`` works."""
        return iter(self._pairs)

    def __len__(self):
        return len(self._pairs)

    def __eq__(self, other):
        if isinstance(other, Headers):
            return self._pairs == other._pairs
        return NotImplemented

    def __repr__(self):
        return "Headers({!r})".format(list(self._pairs))


class Message:
    """A delivery, with what saguin stamped on it already read.

    It wraps the paho message rather than replacing it: ``.paho`` is the
    object paho handed over, unchanged, and topic, payload, qos, retain,
    dup, mid and properties are all here under their paho names.

    One name means something different from paho's, and it is deliberate.
    ``timestamp`` here is **the broker's receipt time**, which is what a
    consumer asking how old a reading is wants; paho's own ``timestamp``
    is when this process received the packet, and is still there on
    ``.paho.timestamp``.
    """

    __slots__ = ("paho", "_props", "headers", "dlq", "_client", "answered")

    def __init__(self, msg, client=None):
        self.paho = msg
        self._client = client
        self.answered = False
        """True once ``queue.ack`` or ``queue.nack`` has answered this job.

        A queue job is answered by a verb of its own rather than by the
        reading loop moving on, so this is what tells a job that was
        handled from one that was abandoned - and only the second is worth
        warning about."""
        self._props = _user_properties(getattr(msg, "properties", None))
        self.headers = Headers(
            (k, v) for k, v in self._props if not k.startswith(RESERVED_PREFIX)
        )
        self.dlq = self._dead_letter()
        """A DeadLetter where this record failed out of a queue, else None."""

    def _dead_letter(self):
        found = {
            k: v for k, v in self._props if k.startswith("saguin-dlq-")
        }
        if "saguin-dlq-channel" not in found:
            return None
        offset = found.get("saguin-dlq-offset")
        attempts = found.get("saguin-dlq-attempts")
        return DeadLetter(
            channel=found["saguin-dlq-channel"],
            offset=None if offset is None else int(offset),
            attempts=None if attempts is None else int(attempts),
            reason=found.get("saguin-dlq-reason"),
            at=_a_moment(found.get("saguin-dlq-at")),
            first=_a_moment(found.get("saguin-dlq-first")),
            last=_a_moment(found.get("saguin-dlq-last")),
        )

    # -- what paho already has, under paho's own names ------------------

    @property
    def topic(self):
        return self.paho.topic

    @property
    def payload(self):
        return self.paho.payload

    @property
    def qos(self):
        return self.paho.qos

    @property
    def retain(self):
        return self.paho.retain

    @property
    def dup(self):
        return self.paho.dup

    @property
    def mid(self):
        return self.paho.mid

    @property
    def properties(self):
        return getattr(self.paho, "properties", None)

    # -- what saguin stamped --------------------------------------------

    def _reserved(self, name):
        for k, v in self._props:
            if k == name:
                return v
        return None

    @property
    def id(self):
        """The record's Message ID: stable across redelivery, dead-lettering
        and replay, which is what makes deduplicating on it work.  Not the
        MQTT packet identifier, which is per-hop and reused constantly."""
        return self._reserved("saguin-id")

    @property
    def offset(self):
        """The record's position in its channel, or None off a channel.

        Deduplicating on this - ignore anything at or below the highest
        you have processed on that channel - is what makes at-least-once
        exact for a consumer that reconnects.
        """
        raw = self._reserved("saguin-offset")
        return None if raw is None else int(raw)

    @property
    def timestamp(self):
        """When the broker received the record, as an aware UTC datetime.

        None where the broker sent none: a record restored from a file
        written before saguin stamped these carries no claim about its age
        rather than a wrong one.
        """
        raw = self._reserved("saguin-timestamp")
        if raw is None:
            return None
        return _EPOCH + timedelta(milliseconds=int(raw))

    @property
    def channel(self):
        """Which channel the record is in - present only where the
        consumer's filter reaches more than one, since a consumer whose
        filter reaches exactly one already knows and would pay 26 bytes a
        message to be told.  None is not "no channel"; it is "you did not
        need telling"."""
        return self._reserved("saguin-channel")

    @property
    def attempt(self):
        """Which delivery attempt of a queue job this is, starting at 1.
        None off a queue."""
        raw = self._reserved("saguin-attempt")
        return None if raw is None else int(raw)

    @property
    def is_catch_up(self):
        """True where this is state the consumer is catching up on rather
        than a change that has just happened.

        It is the RETAIN flag, named for what it means here: a `latest`
        channel sends the current value of every topic a filter reaches at
        subscribe with the flag set, and every change after that without
        it.
        """
        return bool(self.paho.retain)

    @property
    def schema(self):
        """The topic of the schema this payload was written against, or
        None. An ordinary User Property, so it is among the publisher's own
        headers too."""
        return self.headers.get("schema")

    @property
    def deserialized(self):
        """The payload read through the schema it names.

        Raises where the record names no schema, where this client was not
        told which channel schemas live in, or where the pointer falls
        outside that registry - a consumer must not follow a pointer out
        of its own registry, since a publisher may name any topic.
        """
        from . import schemas

        pointer = self.schema
        if not pointer:
            raise schemas.SchemaError(
                "this record names no schema, so there is nothing to read it "
                "through"
            )
        if self._client is None:
            raise schemas.SchemaError(
                "this record was not read through a saguin client, so there is "
                "nothing here that can fetch its schema"
            )
        text = self._client.schema_text(pointer)
        content_type = getattr(self.properties, "ContentType", None)
        return schemas.deserialize(self.payload, pointer, text, content_type)

    def __repr__(self):
        return "Message(topic={!r}, id={!r}, offset={!r})".format(
            self.topic, self.id, self.offset
        )
