"""The client: a paho client with saguin's verbs beside paho's own.

``saguin.Client`` **subclasses** ``paho.mqtt.client.Client``, and paho's
methods are left exactly as they are.  Connecting, TLS, credentials, the
network loop, automatic reconnect, ``publish`` and ``subscribe`` all
behave as they always have - a **broadcast** topic, one no channel
claims, is ordinary MQTT and is published to the ordinary way.

What this adds sits beside them, grouped by the kind of channel it works
on: ``client.append``, ``client.latest``, ``client.queue`` and
``client.admin``.  Those name a *channel*, and the library works out the
MQTT: it asks the broker what the channel is, builds the topic from the
channel's filter and the key you gave, and puts a record id on every
write.
"""

import copy
import json
import logging
import os
import queue
import threading
import time
import uuid

from paho.mqtt import client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from . import schemas
from .channels import (
    ChannelInfo,
    compose,
    declarations,
    inside,
    subscriptions,
)
from .message import Message, _user_properties
from .verbs import Admin, Append, Latest, Queue

_log = logging.getLogger("saguin")

# saguin is MQTT 5 only through this library.  3.1.1 has no User
# Properties, no Response Topic, no shared subscriptions and no session
# expiry, which is every saguin feature - a 3.1.1 client could only
# publish, and the broker goes on admitting 3.1.1 publishers without a
# library to do it.
PROTOCOL = mqtt.MQTTv5

# Where a client asks what a channel is; the name goes on the end.
CATALOGUE_TOPIC = "$saguin/catalogue/"
# Where it reads one value from a `latest` channel without subscribing.
KV_GET_TOPIC = "$saguin/kv/get"
# Where it asks the broker to hang another client up.
DISCONNECT_TOPIC = "$saguin/sessions/disconnect"
# Where a consumer moves its own position; the channel goes in the middle.
SEEK_TOPIC = "$saguin/consumer/{}/seek"

# A day: long enough that an overnight outage, a reboot or a redeploy
# resumes where it stopped, short enough that a client which is never
# coming back stops costing the broker a stored position within a day.
# The broker's own ceiling is the operator's and is longer.
DEFAULT_SESSION_EXPIRY = 24 * 60 * 60

_UNSET = object()

# How many records that reached no reader are held before the oldest is
# dropped. Bounded because a client that neither consumes nor sets
# `on_message` would otherwise grow a queue nobody empties; the drop is
# logged, because a record lost in silence is the failure this library is
# written against.
UNREAD_BOUND = 10000

# How long closing a reader waits for the broker to confirm the
# unsubscribe. Long enough for any answer that is coming, and short
# enough that closing a reader on a link that has silently gone is not a
# hang: what is lost by giving up is that a record may still arrive, and
# that record is kept rather than dropped.
UNSUBSCRIBE_TIMEOUT = 5.0

# Which verbs belong to which kind of channel, so that a refusal can say
# what to use instead rather than only what was wrong.
VERBS_OF = {
    "append": "client.append.publish(), .consume() or .seek()",
    "latest": "client.latest.set(), .get(), .delete() or .consume()",
    "queue": "client.queue.publish(), .fetch(), .work(), .ack(), .nack() "
             "or .redrive()",
}


def _a(kind):
    return "an " + kind if kind[0] in "aeiou" else "a " + kind


class UnknownChannel(Exception):
    """The broker answered nothing about a channel.

    **It says both things it could mean, because the broker does not say
    which** - a channel that does not exist and one this client holds no
    verb on are answered identically, so that asking cannot be used to
    discover what a broker has.
    """

    def __init__(self, name):
        self.name = name
        super().__init__(
            "the broker knows no channel {!r} that this client may use: either "
            "there is no such channel, or this client's roles grant nothing "
            "on it".format(name)
        )


class WrongChannelType(TypeError):
    """This verb is for a different kind of channel.

    It names what the channel actually is and which verbs it takes,
    because the three writes are one publish underneath and this is the
    only thing that tells them apart - the alternative is work quietly
    appended to a log by somebody who meant to queue it.
    """

    def __init__(self, info, want):
        self.channel = info.name
        self.type = info.type
        super().__init__(
            "{!r} is {} channel, and this verb is for {} one - use {}".format(
                info.name, _a(info.type), _a(want), VERBS_OF[info.type]
            )
        )


class RequestRefused(Exception):
    """The broker refused a request in its PUBACK.

    Every question this library asks the broker rides a QoS 1 publish, and
    what is wrong with one comes back as a reason code and the broker's
    own sentence - never on the reply topic, which carries an answer and
    nothing else so that an empty answer can mean one thing.
    """

    def __init__(self, about, reason_code, reason_string):
        self.reason_code = reason_code
        self.reason_string = reason_string
        super().__init__(
            "the broker refused {}: {}{}".format(
                about, reason_code, " - " + reason_string if reason_string else ""
            )
        )


class ConnectRefused(Exception):
    """The broker answered the CONNECT with a failure reason code."""

    def __init__(self, reason_code):
        self.reason_code = reason_code
        super().__init__("the broker refused the connection: {}".format(reason_code))


class SubscriptionRefused(Exception):
    """At least one filter in a SUBSCRIBE came back refused.

    Raised rather than returned, because this is the failure that looks
    exactly like success: MQTT answers each filter separately and a client
    that does not read the codes sits connected, subscribed to nothing,
    and receives nothing for ever.
    """

    def __init__(self, refused, reason_codes, reason_string=None):
        self.refused = tuple(refused)
        self.reason_codes = tuple(reason_codes)
        self.reason_string = reason_string
        """What the broker said, or None where it said nothing.

        **The sentence, not the code.** `0x83` spelled out is
        "Implementation specific error", which tells the person reading it
        nothing at all - saguin sends a sentence beside it naming the rule
        that was broken, and this is where it arrives."""
        said = "the broker refused " + ", ".join(
            "{!r}: {}".format(f, c) for f, c in self.refused
        )
        if reason_string:
            said += " - " + reason_string
        super().__init__(said)


def new_message_id():
    """A UUIDv7: 48 bits of Unix milliseconds, then random, so that ids
    sort by the order they were minted in.

    The broker accepts any string here and generates one itself when a
    publisher sends none.  What makes a client-supplied id worth having is
    that it is the *same* id on a retry - the record identity a consumer
    deduplicates on outlives the packet.
    """
    raw = bytearray(int(time.time() * 1000).to_bytes(6, "big") + os.urandom(10))
    raw[6] = (raw[6] & 0x0F) | 0x70  # version 7
    raw[8] = (raw[8] & 0x3F) | 0x80  # variant 10
    return str(uuid.UUID(bytes=bytes(raw)))


class PublishInfo:
    """What ``publish`` answers: paho's own result, plus the id that went
    on the wire.

    The id is the reason this exists.  paho's ``MQTTMessageInfo`` carries
    ``__slots__`` and cannot be told anything new, and a publisher that
    cannot learn the id its record was stored under cannot log it, cannot
    correlate it and cannot ask about it later.  Everything paho's object
    does, this does: ``rc, mid = info`` still unpacks, and
    ``wait_for_publish`` still blocks until the PUBACK.
    """

    __slots__ = ("paho", "saguin_id")

    def __init__(self, info, saguin_id):
        self.paho = info
        self.saguin_id = saguin_id

    @property
    def mid(self):
        return self.paho.mid

    @property
    def rc(self):
        # Read through rather than copied: paho updates it as the publish
        # progresses, and a copy taken at return would freeze the answer.
        return self.paho.rc

    def is_published(self):
        return self.paho.is_published()

    def wait_for_publish(self, timeout=None):
        return self.paho.wait_for_publish(timeout)

    def __iter__(self):
        return iter(self.paho)

    def __next__(self):
        return next(self.paho)

    def next(self):
        return self.paho.next()

    def __getitem__(self, index):
        return self.paho[index]

    def __str__(self):
        return str((self.rc, self.mid, self.saguin_id))




class Client(mqtt.Client):
    """A paho MQTT 5 client that also speaks in saguin channels.

        client = saguin.Client("gateway-1")
        client.start("broker.local", 1883)
        client.append.publish("readings",
                              key=["site42", "device", "temp/1"],
                              value=b"21.5")

    **Reading needs a durable client**, which is one that keeps its place:

        reader = saguin.Client("orders-reader", durable=True)
        reader.start("broker.local", 1883)
        for record in reader.append.consume("readings"):
            handle(record)

    A durable client is clean start off, a session expiry that is not
    zero, and the same client id next time - the three things together are
    what the broker stores a position against. paho gets two of them wrong
    by default, in the direction that loses the position: its
    ``clean_start`` means *clean on the first connect*, so a client
    restarting after a crash wipes the position it restarted to resume
    from, and it sends no session expiry unless told, which means zero.

    Two defaults differ from paho's, and both are saguin's answer rather
    than a preference:

    * **The protocol is MQTT 5** and nothing else is accepted.
    * **A publish is QoS 1** unless the caller asks for another, because
      every refusal saguin gives a publisher comes back as a PUBACK reason
      code - and at QoS 0 there is no PUBACK, so all of them arrive as
      silence.
    """

    def __init__(
        self,
        client_id,
        durable=False,
        session_expiry=DEFAULT_SESSION_EXPIRY,
        schema_registry=None,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        clean_session=None,
        userdata=None,
        protocol=PROTOCOL,
        transport="tcp",
        reconnect_on_failure=True,
        manual_ack=_UNSET,
        **kw
    ):
        # **Every refusal below comes after the base class is built.**
        # paho's ``__del__`` reaches for attributes its ``__init__`` sets,
        # so an object abandoned half-built raises a second, unrelated
        # error out of the garbage collector whenever the interpreter gets
        # round to it - on top of the clear one raised here.
        #
        # Manual acknowledgement by default, because reading acknowledges
        # a record when the loop asks for the next one rather than when it
        # arrives: the stored position advances on the acknowledgement, so
        # acknowledging on arrival would let a client that died half way
        # through a record resume *after* it.
        self._manual_ack = True if manual_ack is _UNSET else bool(manual_ack)
        super().__init__(
            callback_api_version,
            client_id=client_id or "",
            clean_session=clean_session,
            userdata=userdata,
            protocol=protocol,
            transport=transport,
            reconnect_on_failure=reconnect_on_failure,
            manual_ack=self._manual_ack,
            **kw
        )
        if protocol != PROTOCOL:
            raise ValueError(
                "saguin.Client speaks MQTT 5 and nothing else; for a 3.1.1 "
                "publisher use paho.mqtt.client.Client directly"
            )
        if not client_id:
            raise ValueError(
                "a saguin client needs an id of its own: the broker stores a "
                "durable client's position against it, so a generated one is a "
                "new client every time and the position is never found again"
            )
        if durable and not session_expiry:
            raise ValueError(
                "a session expiry of zero ends the session with the connection "
                "and stores no position, so it cannot be durable"
            )

        self.name = client_id
        self.schema_registry = schema_registry
        """The channel schemas are registered in, named once here.

        Deserializing needs it, and not as a convenience: a consumer must not
        follow a schema pointer out of its registry, since an ACL governs
        who may *write* a topic and never who may *name* one."""
        self.durable = bool(durable)
        self.session_expiry = int(session_expiry) if durable else 0
        """What this client asks for.  What it was granted is
        `granted_session_expiry`, and they differ when the broker's cap is
        lower."""
        self.granted_session_expiry = None
        self.session_present = None
        """Whether the broker found this client's session.  False on a
        first connection - and also when a stored position has fallen
        below a channel's retention floor, which is the broker saying it
        cannot resume you without a silent gap."""
        self.connect_reason_code = None

        self.append = Append(self)
        self.latest = Latest(self)
        self.queue = Queue(self)
        self.admin = Admin(self)

        self._known = {}
        self._schemas = {}
        self._routing = {}
        self._asked = 0
        self._answers = {}
        # **Everything that reaches no reader.** A resumed session is
        # served records for subscriptions made on a previous connection -
        # before the application has called `consume`, so before any route
        # for them exists. With nowhere to put those, a durable consumer
        # loses exactly what it reconnected for. They wait here, and
        # `consume` sweeps out the ones that are its own.
        self._unread = queue.Queue()
        # **What holds the buffer still while it is rearranged.** Two
        # places take the buffer apart and put part of it back - `consume`
        # sweeping out its own records, and a reader handing back what it
        # still held - and both run on the caller's thread while paho's
        # network thread goes on adding to the tail. Without this, a
        # record arriving during either one lands in front of records
        # older than itself, and the reader that picks them up is handed
        # them out of order. Driven rather than reasoned about: at full
        # publishing rate the reader hand-back inverted the buffer in five
        # runs out of ten, and the `consume` sweep inverted a buffer of
        # three thousand in one run out of three.
        self._holding = threading.Lock()
        # **Which connection a record arrived on.** A packet identifier is
        # the broker's, per connection, and it starts again at 1 on the
        # next one - so an identifier from a dead connection acknowledges
        # whatever the live connection happens to be holding under that
        # number. Counted here, carried beside every buffered record, and
        # checked before anything is acknowledged.
        self._generation = 0
        self._subacks = queue.Queue()
        self._unsubacks = queue.Queue()
        self._connacks = queue.Queue()
        # A topic of this client's own, never published to: the broker
        # writes an answer to the asking connection rather than publishing
        # it, so this needs no subscription and nobody else can receive
        # it.  Unique per client, so paho's per-topic routing here cannot
        # swallow anything the caller is reading.
        self._reply_topic = "saguin/reply/" + uuid.uuid4().hex
        self.message_callback_add(self._reply_topic, self._took_answer)
        # **Stored under names of this library's own.** paho backs each
        # callback property with the same name under one underscore -
        # ``_on_connect``, ``_on_publish`` and the rest - so keeping the
        # caller's there makes a handler its own caller. It did: reading
        # went through ``on_message`` at the time, and the first message
        # received recursed until the interpreter stopped it.
        self._saguin_on_connect = None
        self._saguin_on_subscribe = None
        self._saguin_on_unsubscribe = None
        self._saguin_on_message = None
        self._saguin_on_publish = None
        self._pubacks = {}
        # **Held across the publish and the registering of its waiter.**
        # A PUBACK arrives on the network thread and can beat the line
        # after `publish` that says who is waiting for it - against a
        # broker on the same machine it regularly does - and the answer
        # was then dropped and the ask waited out its timeout. The lock is
        # what makes "publish, then wait" one step.
        self._asking = threading.Lock()

    # -- paho's callbacks, chained so the caller keeps its own -----------

    # Three of paho's callbacks keep a handler of this library's in front
    # of the caller's, because each carries an answer this library needs:
    # the CONNACK says what session expiry was granted and whether the
    # session was found, the SUBACK says which filters were refused, and
    # the PUBACK says whether a question was refused and in what words.
    #
    # **`on_message` is not among them, and used to be.** Reading went
    # through it while every reader drew from one queue of arrivals per
    # client - which is exactly what let two readers take each other's
    # records. `consume` registers paho's own per-topic callbacks now, so
    # `on_message` is the caller's alone and paho holds it.
    #
    # Assigning any of the three works as it does on a paho client: the
    # caller's is stored and called after the library's. Reading one
    # answers the library's handler rather than the caller's, which is
    # what makes paho call it.

    @property
    def on_connect(self):
        return self._connected

    @on_connect.setter
    def on_connect(self, func):
        self._saguin_on_connect = func

    @property
    def on_subscribe(self):
        return self._subscribed

    @on_subscribe.setter
    def on_subscribe(self, func):
        self._saguin_on_subscribe = func

    @property
    def on_unsubscribe(self):
        return self._unsubscribed

    @on_unsubscribe.setter
    def on_unsubscribe(self, func):
        self._saguin_on_unsubscribe = func

    @property
    def on_message(self):
        return self._took_message

    @on_message.setter
    def on_message(self, func):
        self._saguin_on_message = func

    @property
    def on_publish(self):
        return self._published

    @on_publish.setter
    def on_publish(self, func):
        self._saguin_on_publish = func

    def _connected(self, client, userdata, flags, reason_code, properties=None):
        self._known.clear()
        self._generation += 1
        granted = getattr(properties, "SessionExpiryInterval", None)
        self.connect_reason_code = reason_code
        self.session_present = bool(getattr(flags, "session_present", False))
        self.granted_session_expiry = (
            self.session_expiry if granted is None else granted
        )
        self._connacks.put(reason_code)
        if self._saguin_on_connect is not None:
            self._saguin_on_connect(client, userdata, flags, reason_code, properties)

    def _subscribed(self, client, userdata, mid, reason_codes, properties=None):
        # **The properties travel with the codes.** A refused SUBSCRIBE
        # carries saguin's own sentence in the SUBACK's Reason String, and
        # dropping it here left `SubscriptionRefused` able to say only
        # "Implementation specific error" about a broker that had
        # explained itself.
        self._subacks.put((mid, reason_codes, properties))
        if self._saguin_on_subscribe is not None:
            self._saguin_on_subscribe(client, userdata, mid, reason_codes, properties)

    def _unsubscribed(self, client, userdata, mid, reason_codes=None,
                      properties=None):
        self._unsubacks.put(mid)
        if self._saguin_on_unsubscribe is not None:
            self._saguin_on_unsubscribe(
                client, userdata, mid, reason_codes, properties)

    def _took_message(self, client, userdata, msg):
        """Anything paho routed to nobody: no reader's filter matched it.

        **Kept rather than dropped, and bounded rather than kept for
        ever.** What lands here is a resumed session's records before
        `consume` has been called for them, and broadcast a caller reads
        through `on_message`. A client doing neither would otherwise grow
        a queue nobody empties, so it is capped - and says so, because a
        record dropped in silence is the failure this library is written
        against.
        """
        dropped = None
        # **Held, so that this record cannot land in front of an older
        # one.** `consume` and a closing reader both empty the buffer and
        # put part of it back, and this runs on paho's network thread
        # while they run on the caller's.
        with self._holding:
            if self._unread.qsize() >= UNREAD_BOUND:
                try:
                    _, dropped = self._unread.get_nowait()
                except queue.Empty:
                    pass
            self._unread.put((self._generation, msg))
        if dropped is not None:
            _log.warning(
                "dropping %r: %d records have reached no reader on %r and "
                "nothing is reading them - subscribe with consume(), or set "
                "on_message", dropped.topic, UNREAD_BOUND, self.name)
        if self._saguin_on_message is not None:
            self._saguin_on_message(client, userdata, msg)

    def _published(self, client, userdata, mid, reason_code=None, properties=None):
        with self._asking:
            waiting = self._pubacks.get(mid)
        if waiting is not None:
            waiting.put((reason_code, getattr(properties, "ReasonString", None)))
        if self._saguin_on_publish is not None:
            self._saguin_on_publish(client, userdata, mid, reason_code, properties)

    def _took_answer(self, client, userdata, msg):
        waiting = self._answers.get(getattr(msg.properties, "CorrelationData", None))
        if waiting is not None:
            waiting.put(msg)

    # -- connecting ------------------------------------------------------

    def connect(self, host, port=1883, keepalive=60, *a, **kw):
        """paho's connect, with what durability needs filled in.

        A durable client must send clean start off and a session expiry or
        the broker stores nothing, and a caller should not have to
        remember that. Nothing else changes: this still returns at once
        and answers what paho answers.
        """
        if self.durable:
            if kw.get("clean_start"):
                raise ValueError(
                    "a durable client connects with clean start off, or the "
                    "broker discards the position it connected to resume from"
                )
            kw["clean_start"] = False
            props = copy.copy(kw.get("properties")) or Properties(PacketTypes.CONNECT)
            props.SessionExpiryInterval = self.session_expiry
            kw["properties"] = props
        return super().connect(host, port, keepalive, *a, **kw)

    def start(self, host, port=1883, keepalive=60, timeout=10, **kw):
        """Connect, run the network loop, and wait for the broker's answer.

        One call instead of three, because everything this client needs to
        know is in the CONNACK - whether its session was found, and how
        long its position will be kept - and none of the verbs can work
        before the loop is running. paho's own `connect` and `loop_start`
        are still there for anyone who wants them.
        """
        rc = self.connect(host, port, keepalive, **kw)
        self.loop_start()
        code = self._connacks.get(timeout=timeout)
        if getattr(code, "is_failure", False):
            raise ConnectRefused(code)
        return rc

    def close(self):
        """Stop the loop and disconnect. A durable client's session, and
        the position in it, stay with the broker for the granted expiry."""
        self.loop_stop()
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- what a channel is ----------------------------------------------

    def channel(self, name, timeout=10):
        """What the broker says this channel is, asked once and remembered.

        The answer holds the channel's type and the topic filter it
        claims, which is the one thing about a channel a client cannot
        work out for itself. It cannot go stale while this connection
        lasts: channels are fixed when the broker starts, so a broker
        whose channels changed dropped this connection on the way.

        Raises `UnknownChannel` where the broker answers nothing, and
        remembers no such answer - so a grant that arrives later is picked
        up by simply asking again.
        """
        known = self._known.get(name)
        if known is not None:
            return known
        reply = self._ask(CATALOGUE_TOPIC + name, b"", timeout,
                          "what channel {!r} is".format(name))
        if not reply.payload:
            raise UnknownChannel(name)
        found = ChannelInfo.from_json(json.loads(reply.payload))
        self._known[name] = found
        return found

    def record(self, raw):
        """A raw paho message, as a saguin record.

        **For broadcast**, which is the one path this library does not
        hand you a record on: a topic no channel claims is ordinary MQTT,
        read through paho's own `subscribe` and `on_message`, and what
        arrives there is paho's message rather than one of these.

            def on_message(client, userdata, msg):
                record = client.record(msg)
                print(record.deserialized)

        `saguin.Message(msg, client)` is the same thing written out. This
        exists because the second argument is easy to leave off, and a
        record built without it raises when you deserialize it - the client is
        what fetches the schema.
        """
        return Message(raw, self)

    def forget_schema(self, topic=None):
        """Drop a remembered schema, or all of them, so the next read asks
        the broker again.

        Needed only where a schema is republished at a topic this client
        has already read - which the convention says to avoid, since the
        topic is the identity and a new version is a new topic.
        """
        if topic is None:
            self._schemas.clear()
        else:
            self._schemas.pop(topic, None)

    def forget_channel(self, name):
        """Drop what was remembered about one channel, so the next call
        asks again.

        What a channel *is* cannot change under a live connection, but
        what this client may do there can - an `acl_file` is re-read on
        SIGUSR1. A refused publish or subscribe drops the entry through
        this.
        """
        self._known.pop(name, None)

    # -- what the verbs are made of --------------------------------------

    def _ask(self, topic, payload, timeout, about):
        """Ask the broker a question and wait for the answer it writes back
        to this connection.

        No subscription is involved: the reply is written to the asking
        socket, so nobody else can receive it. **The PUBACK is read first**,
        because that is where a refused request is answered - a request the
        broker refused never produces a reply, and waiting for one would
        turn the broker's own sentence into a timeout saying nothing.
        """
        # **Said here rather than waited out.** Every question needs an
        # answer written back to this connection, so on a client that has
        # closed there is nothing to wait for - and "the broker did not
        # acknowledge within 10s" is a misleading way to say "you are not
        # connected", especially for `record.deserialized`, which reaches for a
        # schema and can easily be called after the loop it was read in.
        if not self.is_connected():
            raise RuntimeError(
                "this client is not connected, so it cannot ask the broker {} - "
                "a question needs a connection for the answer to arrive "
                "on".format(about)
            )
        self._asked += 1
        corr = "saguin-{}".format(self._asked).encode()
        answers = self._answers.setdefault(corr, queue.Queue())
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic = self._reply_topic
        props.CorrelationData = corr
        try:
            with self._asking:
                info = mqtt.Client.publish(
                    self, topic, payload, qos=1, properties=props
                )
                acks = self._pubacks.setdefault(info.mid, queue.Queue())
            try:
                try:
                    code, sentence = acks.get(timeout=timeout)
                except queue.Empty:
                    raise TimeoutError(
                        "the broker did not acknowledge {} within {}s".format(
                            about, timeout
                        )
                    )
            finally:
                self._pubacks.pop(info.mid, None)
            if getattr(code, "is_failure", False):
                raise RequestRefused(about, code, sentence)
            try:
                return answers.get(timeout=timeout)
            except queue.Empty:
                raise TimeoutError(
                    "the broker did not answer {} within {}s".format(about, timeout)
                )
        finally:
            self._answers.pop(corr, None)

    @staticmethod
    def _must_be(info, want):
        if info.type != want:
            raise WrongChannelType(info, want)

    def _write(self, want, channel, key, value, headers, **kw):
        info = self.channel(channel, timeout=kw.pop("timeout", 10))
        self._must_be(info, want)
        return self.publish(compose(info, key), payload=value, headers=headers, **kw)

    def _point_read(self, info, key, timeout):
        return self._read_topic(compose(info, key), timeout)

    def _read_topic(self, topic, timeout=10):
        """One topic's current value, by topic rather than by channel and
        key - which is what a schema pointer is."""
        reply = self._ask(KV_GET_TOPIC, topic.encode(), timeout,
                          "a read of {!r}".format(topic))
        return reply.payload if reply.payload else None

    def schema_text(self, topic, timeout=10):
        """The schema registered at a topic, read once and remembered.

        **Refused where the topic falls outside the registry**, which is
        the rule that makes it safe to follow a pointer somebody else put
        in a message: a publisher may name any topic, including a `latest`
        channel holding device state, and a consumer that followed one
        would read it out.
        """
        if not self.schema_registry:
            raise schemas.SchemaError(
                "this client was not told which channel schemas live in, so it "
                "cannot follow a schema pointer; build it with "
                "schema_registry=<channel>"
            )
        registry = self.channel(self.schema_registry, timeout=timeout)
        # **Checked, because pointing this at the wrong kind of channel
        # fails later and confusingly.** A registry is a key-value store:
        # a schema is set at a topic, read back one at a time, and retired
        # by deleting it. An append channel would take the writes and
        # answer no read.
        if registry.type != "latest":
            raise schemas.SchemaError(
                "{!r} is {} channel, and a schema registry is a latest one - "
                "it is a key-value store of schema texts".format(
                    self.schema_registry, _a(registry.type)
                )
            )
        if not schemas.inside(registry.filter, topic):
            raise schemas.SchemaError(
                "{!r} is outside {!r}, whose filter is {!r} - a schema pointer "
                "is followed only inside its own registry".format(
                    topic, self.schema_registry, registry.filter
                )
            )
        # **Kept for the life of the connection, and a republished schema
        # is not noticed.** That is a limit rather than an oversight, and
        # the reason is Avro: schemaless deserializing cannot reliably tell
        # that it has the wrong schema - adding a field with a default
        # leaves an older reader parsing the front of the bytes and
        # ignoring the rest, with no error to trigger a re-read on. So
        # there is no clever invalidation here that would only work for
        # protobuf.
        #
        # It costs nothing where the convention is followed, because the
        # topic is the identity: **a new version is a new topic**, and a
        # new topic is a cache miss. `forget_schema` is for anybody who
        # republishes at the same one anyway.
        known = self._schemas.get(topic)
        if known is not None:
            return known
        text = self._read_topic(topic, timeout)
        if not text:
            raise schemas.SchemaError(
                "no schema is registered at {!r}".format(topic)
            )
        text = text.decode()
        if len(self._schemas) > 200:
            self._schemas.clear()
        self._schemas[topic] = text
        return text

    def _answer_job(self, job, outcome):
        reply_to = getattr(job.properties, "ResponseTopic", None)
        correlation = getattr(job.properties, "CorrelationData", None)
        if not reply_to or not correlation:
            raise ValueError(
                "this is not a queue delivery: it carries no Response Topic and "
                "Correlation Data to answer with"
            )
        props = Properties(PacketTypes.PUBLISH)
        props.CorrelationData = correlation
        info = mqtt.Client.publish(self, reply_to, outcome, qos=1, properties=props)
        # **Marked answered here**, so that leaving the reading loop after
        # acknowledging a job - which is an ordinary thing to do and what
        # this library's own examples show - is not reported as
        # abandoning it.
        job.answered = True
        return info

    def _disconnect_client(self, client_id, timeout):
        reply = self._ask(DISCONNECT_TOPIC, str(client_id).encode(), timeout,
                          "a request to hang up {!r}".format(client_id))
        return reply.payload.decode()

    def _seek(self, channel, to, timeout=10):
        """Move this client's position in one channel, and answer where it
        landed.

        The value is the broker's own: an integer offset - `0` the
        retention floor, `-1` the next offset, or a position - or a string
        holding a duration such as `12h` or `7d`, or an RFC 3339 moment.
        It is passed through unchanged and never guessed at: **a bare
        integer always means an offset**, because `1763000000` is a
        plausible offset and a plausible Unix time, and seeking to the
        wrong one reads on in order and reports success.
        """
        info = self.channel(channel, timeout=timeout)
        self._must_be(info, "append")
        if not self.durable:
            raise ValueError(
                "seeking moves a stored position, and a client that keeps no "
                "place has none; build it with durable=True"
            )
        reply = self._ask(
            SEEK_TOPIC.format(channel), str(to).encode(), timeout,
            "a seek of {!r} to {!r}".format(channel, to),
        )
        return int(reply.payload)

    def _consume(self, want, channel, key, qos=1, timeout=None, ask_timeout=10,
                 start=None, topic_hash=None, saguin_filter=None):
        """Subscribe, and yield what arrives **on these filters**.

        **Each reader gets its own arrivals**, routed by paho's own
        per-topic callbacks rather than out of one queue per client. With
        one queue, two readers on the same client take each other's
        records: whichever asks for the next one first is handed whatever
        arrived, whatever it subscribed to. That is not a tidiness
        problem - the reader then acknowledges a record belonging to the
        other subscription, which is a record acknowledged away from a
        consumer that never saw it.
        """
        declared = declarations(topic_hash=topic_hash, saguin_filter=saguin_filter)
        if declared and want == "queue":
            # **Refused before anything is asked of the broker**, which
            # refuses it too: a queue already divides its work between its
            # workers, so a slice of one is a second answer to the
            # question the channel type exists to answer. Saying so here
            # names the call that did it rather than handing back a
            # reason code from a SUBACK.
            raise ValueError(
                "a queue cannot be sliced: it already hands each job to one "
                "worker, so declaring a slice of {!r} would be two "
                "mechanisms dividing one stream".format(channel)
            )
        info = self.channel(channel, timeout=ask_timeout)
        self._must_be(info, want)
        # **Before the subscribe, and only where there is no position.**
        # Before, or records from the old position are already in flight
        # and race the seek. Only where there is none, because a value
        # written once in the code would otherwise fire on every restart -
        # a client asking for the floor would replay the whole channel
        # every Monday. `session_present` is how the broker says it has no
        # session for this id, and so no position.
        #
        # It cannot see one case: a session that exists but has never read
        # *this* channel. There is no verb for "where am I here", so the
        # channel's own `start:` setting decides that one.
        if start is not None and want == "append" and not self.session_present:
            self._seek(channel, start, timeout=ask_timeout)
        if info.pin:
            # **What is subscribed to and what arrives are not the same
            # here.** A queue is joined through its pin, and its records
            # arrive on the queue's own topics - so routing on the pin
            # would match nothing that is ever delivered.
            filters, arriving = [info.pin], [info.filter]
        else:
            filters = arriving = subscriptions(info, key)

        # **A second reader of a filter takes it over, and the first is
        # closed rather than left quietly empty.** paho keys a topic
        # callback by its filter, so two readers of one filter cannot both
        # hold it - and reading a channel, seeking, and reading it again
        # is an ordinary thing to do, so refusing the second was wrong.
        # What must not happen is two readers interleaving on one filter,
        # and ending the first is what stops that.
        for one in arriving:
            held = self._routing.get(one)
            if held is not None:
                held.close()

        mine = queue.Queue()

        # **Swept before subscribing.** A resumed session is served
        # records for subscriptions made on a previous connection, before
        # this reader existed - so they are waiting with everything else
        # that reached nobody, and they belong here.
        #
        # **Held for the whole sweep**, because taking the buffer apart
        # and putting part of it back is not one step: a record arriving
        # between the two goes to the tail, and the records put back after
        # it are older than it is. The reader that picks those up is then
        # handed them out of order, which is the one promise an append
        # channel makes.
        now = self._generation
        with self._holding:
            self._sweep(now, mine, arriving)

        # **Routed before the SUBSCRIBE, and before the caller asks for a
        # record.** The broker answers a SUBSCRIBE on a `latest` channel
        # with the current state at once, so a callback added after it
        # misses that - and a generator's body does not run until the
        # first `next()`, later still.
        records = self._records(timeout, mine, arriving, filters,
                                queue_name=channel if info.pin else None)
        for one in arriving:
            self._routing[one] = records
            self.message_callback_add(
                one, lambda cl, u, msg: mine.put((self._generation, msg)))
        try:
            self._subscribe_and_check(filters, qos, ask_timeout, declared)
        except SubscriptionRefused:
            self._stop_routing(arriving)
            # What a channel *is* cannot change under a live connection,
            # but what this client may do there can. So the answer is
            # dropped and the next call asks again, while the broker's own
            # refusal is raised rather than retried against a broker that
            # is saying no on purpose.
            self.forget_channel(channel)
            raise
        return records

    def _sweep(self, now, mine, arriving):
        """Move this reader's records out of the unread buffer.

        Call with ``_holding``.  Everything not this reader's keeps its
        place in the buffer, in the order it arrived.
        """
        for held in _drain(self._unread):
            when, msg = held
            if when != now:
                # **Dropped rather than read.** This record reached nobody
                # on a connection that has since gone, so it was never
                # acknowledged - and a session that survives the drop is
                # sent it again, which is what at-least-once means and
                # what was watched happening. Keeping it would hand the
                # caller the record twice and, worse, acknowledge it with
                # the old connection's packet identifier.
                continue
            (mine if any(inside(f, msg.topic) for f in arriving)
             else self._unread).put(held)

    def _stop_routing(self, arriving, filters=None):
        """Stop routing these filters here, and stop being sent them.

        **Unsubscribing is the half that was missing, and on a queue it
        was taking work nobody would do.** A reader that has finished
        stopped routing but stayed subscribed, so the broker went on
        handing it records - and a queue hands out *leases*: jobs given to
        a client that will never answer, which then time out, are retried,
        and are dead-lettered for no reason anybody could see. On an
        append or `latest` channel it was quieter and still wrong: the
        records piled into the unread buffer.
        """
        for one in arriving:
            self._routing.pop(one, None)
            self.message_callback_remove(one)
        if filters:
            self._stop_being_sent(list(filters))

    def _stop_being_sent(self, filters):
        """Unsubscribe, and wait for the broker to say it took effect.

        **Sending the UNSUBSCRIBE is not the same as being unsubscribed.**
        Until the broker has processed it, a record published in the gap
        is still delivered under the old subscription - so a reader that
        has finished goes on receiving, which on a queue is a job leased
        to a worker that will never answer it. That is the defect the
        unsubscribe was added to fix, alive in a smaller window: driven
        thirty times, two rounds delivered a record after the reader had
        closed.

        **It never raises and it is always bounded.** Closing a reader has
        to work on a connection that has already gone - where no answer is
        coming - and it may be called from paho's own network thread,
        which is the thread that would have delivered the answer, so
        waiting there would wait for itself.
        """
        rc, mid = mqtt.Client.unsubscribe(self, filters)
        if rc != mqtt.MQTT_ERR_SUCCESS or mid is None:
            return
        if threading.current_thread() is getattr(self, "_thread", None):
            return
        deadline = time.monotonic() + UNSUBSCRIBE_TIMEOUT
        others = []
        try:
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    return
                try:
                    got = self._unsubacks.get(timeout=left)
                except queue.Empty:
                    return
                if got == mid:
                    return
                # Another unsubscribe's answer, arriving out of order.
                # Kept rather than dropped, or whoever is waiting for it
                # waits out their whole timeout for an answer that came.
                others.append(got)
        finally:
            for one in others:
                self._unsubacks.put(one)

    def _subscribe_and_check(self, filters, qos, timeout, declared=()):
        """Subscribe and read the SUBACK, raising on a refused filter.

        This is what the verbs use, and it goes to paho's own
        ``subscribe`` rather than to this class's, whose keywords it has
        already turned into ``declared``.  MQTT answers each filter
        separately, and a client that does not read the codes sits
        connected, subscribed to nothing, and receives nothing for ever.

        **A declaration is one set of properties for the whole packet.**
        The filters here are one channel's, expanded from one written
        filter, so they take one slice between them - which is what the
        broker means by the declaration being on the packet rather than
        on a filter.
        """
        _, mid = mqtt.Client.subscribe(self, [(one, qos) for one in filters],
                                       properties=_declaring(None, declared))
        codes, answered = self._suback_for(mid, timeout)
        refused = [
            (f, c) for f, c in zip(filters, codes) if getattr(c, "is_failure", False)
        ]
        if refused:
            # **The granted half is undone before the refusal is raised.**
            # MQTT answers each filter separately, so a SUBSCRIBE that is
            # partly refused leaves the client subscribed to the rest -
            # and to no reader, because the caller is about to be handed
            # an exception instead of one. Those records then arrive for
            # ever and pile into the unread buffer; on a queue they are
            # leases handed to a worker that does not exist.
            #
            # Reachable wherever one filter of a channel is granted and
            # another is not, which is an acl_file narrowing a channel
            # whose filter carries a `{a,b}` - the one place this library
            # sends a SUBSCRIBE naming more than one filter.
            granted = [
                f for f, c in zip(filters, codes)
                if not getattr(c, "is_failure", False)
            ]
            if granted:
                self._stop_being_sent(granted)
            raise SubscriptionRefused(
                refused, codes, getattr(answered, "ReasonString", None))
        return codes

    def _suback_for(self, mid, timeout):
        """The reason codes for this SUBSCRIBE, and the answer's properties."""
        held = []
        try:
            while True:
                got, codes, answered = self._subacks.get(timeout=timeout)
                if got == mid:
                    return codes, answered
                # Another subscription's answer, arriving out of order.
                # Kept rather than dropped: dropping it would hang the
                # call waiting for it.
                held.append((got, codes, answered))
        finally:
            for one in held:
                self._subacks.put(one)

    def _records(self, timeout, mine, arriving, filters,
                 queue_name=None):
        """Yield records, acknowledging each as the next is asked
        for - never as it arrives. The stored position advances on the
        acknowledgement, so acknowledging on arrival would let a client
        that died half way through a record resume *after* it, which is
        the one thing a position must never do.

        **On an append or `latest` channel, leaving the loop early leaves
        the record in hand unacknowledged, and it comes back**: the
        position has not advanced, so the next read is served it again.

        **On a queue it does not come back - not until this client
        disconnects.** A job's lease starts at the acknowledgement, so a
        job that was never acknowledged has no lease to expire and
        nothing brings it round again; it is held for a worker that has
        stopped asking, and the queue quietly stops draining by one job.
        Acknowledge it or hand it back before leaving the loop, which is
        what ``queue.work`` does on every path.
        """
        holding, arrived_on = None, None
        try:
            while True:
                if holding is not None:
                    # **Never acknowledged across a reconnection.** The
                    # packet identifier belongs to the connection that
                    # delivered the record, and the next connection
                    # numbers its own from 1 - so this acknowledgement
                    # would land on whatever that one is holding under the
                    # same number, which is a record acknowledged away
                    # from a consumer that never saw it. The record itself
                    # is not lost: unacknowledged on a session that
                    # survived, the broker sends it again.
                    if self._manual_ack and arrived_on == self._generation:
                        self.ack(holding.mid, holding.qos)
                    holding, arrived_on = None, None
                try:
                    arrived_on, raw = mine.get(timeout=timeout)
                except queue.Empty:
                    return
                holding = Message(raw, self)
                yield holding
        finally:
            # **A queue job in hand is said out loud**, because nothing
            # else would ever say it. It is not lost and it is not
            # duplicated: it is held for this connection, and the queue
            # goes on looking healthy while it is one job short. Silence
            # here is somebody reading a queue's depth and finding no
            # reason for it.
            if (holding is not None and queue_name is not None
                    and not holding.answered):
                _log.warning(
                    "client %r left the reading loop for queue %r holding a job "
                    "that was neither acknowledged nor handed back. It stays "
                    "with this client until it disconnects and no other worker "
                    "is offered it, so the queue is one job short with nothing "
                    "to show for it. Call queue.ack(record) or "
                    "queue.nack(record) before leaving the loop.",
                    self.name, queue_name)
            # **Removed when the reader is done**, including when the
            # caller leaves the loop early - otherwise a filter goes on
            # routing into a queue nobody reads, and the records it takes
            # are records the caller's own `on_message` never sees.
            self._stop_routing(arriving, filters)
            # **And what it was still holding goes back.** A reader closed
            # by the next one - read a channel, seek, read it again - has
            # records in hand that nothing has acknowledged. Dropping them
            # loses what the broker had already sent on this connection,
            # and the reader that follows would wait for records it was
            # never going to be sent twice.
            #
            # **In front of the buffer, not behind it.** Everything this
            # reader is handing back is older than anything the buffer
            # holds for its filters: the route was live until the line
            # above, so those records went here rather than there. What
            # can be in the buffer is what arrived in the gap the line
            # above opens - the route removed, then a whole round trip
            # spent waiting for the broker to confirm the unsubscribe -
            # and putting these behind that is putting them behind records
            # that came after them.
            left = _drain(mine)
            if left:
                with self._holding:
                    after = _drain(self._unread)
                    for one in left:
                        self._unread.put(one)
                    for one in after:
                        self._unread.put(one)

    # -- MQTT's own publish, with a record id on it ----------------------

    def subscribe(self, topic, qos=0, options=None, properties=None,
                  topic_hash=None, saguin_filter=None):
        """paho's own subscribe, and the words a subscriber takes a slice with.

            client.subscribe("iot/site42/#", qos=1, topic_hash=(8, 1))

        ``topic_hash`` is a ``(partitions, index)`` pair, or several of
        them for a member holding more than one slice.  The broker
        delivers a message only where ``topic_hash(topic) % partitions``
        is one of the indices declared, and a client declaring nothing
        gets everything - which is every client that has never heard of
        this.

        **Here as well as on the channel verbs, because broadcast has no
        verb.** A topic no channel claims is ordinary MQTT and is
        subscribed to the ordinary way, so the one place a broadcast
        subscriber could say this is the call it already makes. Everything
        paho's ``subscribe`` does it still does: these are two keywords
        added to the end of its signature, and passing neither reaches it
        unchanged.

        ``saguin_filter`` puts a call this library has not heard of on the
        packet verbatim, so a broker that has grown one does not wait for
        a release here.
        """
        declared = declarations(topic_hash=topic_hash, saguin_filter=saguin_filter)
        return mqtt.Client.subscribe(self, topic, qos, options,
                                     _declaring(properties, declared))

    def publish(
        self,
        topic,
        payload=None,
        qos=_UNSET,
        retain=False,
        properties=None,
        saguin_id=None,
        headers=None,
        schema=None,
    ):
        """Publish to a topic, exactly as paho does, plus a record id.

        This is MQTT's own verb and it takes a **topic**: a broadcast
        topic is published to the ordinary way. To write to a saguin
        channel by name, use ``client.append.publish``,
        ``client.latest.set`` or ``client.queue.publish``, which build the
        topic from the channel's filter.

        **The record id is the library's to handle.** Every publish
        carries a ``saguin-id`` - a UUIDv7 the broker stores as the
        record's Message ID, stable across redelivery, dead-lettering and
        replay, and what a consumer deduplicates on. Supply one with
        ``saguin_id=`` and it is used unchanged, which is what makes a
        retry the same record rather than a second one; supply none and
        one is minted. Either way the id that went out is on the object
        returned.

        The caller's ``properties`` is never modified: a publisher that
        builds one and reuses it would otherwise send the first record's
        id on every record after it.
        """
        if schema is not None:
            payload, content_type = schemas.serialize(
                payload, schema, self.schema_text(schema)
            )
            headers = list(headers or []) + [(schemas.SCHEMA_PROPERTY, schema)]
            properties = copy.copy(properties) if properties is not None else None
            if properties is None:
                properties = Properties(PacketTypes.PUBLISH)
            properties.ContentType = content_type
        if headers is not None:
            properties = _with_headers(properties, headers)
        if qos is _UNSET:
            qos = 1
        props, went_out = self._with_message_id(properties, saguin_id)
        info = super().publish(
            topic, payload=payload, qos=qos, retain=retain, properties=props
        )
        return PublishInfo(info, went_out)

    @staticmethod
    def _with_message_id(properties, saguin_id):
        """The properties to send, and the id they carry.

        Both together, rather than reading the id back off the object
        afterwards: paho carries one User Property as a bare pair and
        several as a list of them, so a reader has to normalize a shape
        this function already knows.
        """
        if properties is None:
            props = Properties(PacketTypes.PUBLISH)
            existing = []
        else:
            # Shallow: User Properties are the only field written here,
            # and _set_user_properties replaces that one outright, so
            # nothing the caller holds is reachable from what goes out.
            props = copy.copy(properties)
            existing = list(_user_properties(properties))

        if saguin_id is None:
            for name, value in existing:
                if name == "saguin-id":
                    return props, value
            saguin_id = new_message_id()

        _set_user_properties(
            props,
            [(name, value) for name, value in existing if name != "saguin-id"]
            + [("saguin-id", saguin_id)],
        )
        return props, saguin_id


def _with_headers(properties, headers):
    """The caller's properties with these headers added as User Properties.

    A copy, never the caller's own object: a publisher that builds one
    Properties and reuses it would otherwise accumulate every message's
    headers on every message after it.
    """
    if not headers:
        return properties
    props = copy.copy(properties) if properties is not None else Properties(
        PacketTypes.PUBLISH
    )
    _set_user_properties(props, list(_user_properties(properties)) + [
        (str(name), str(text)) for name, text in headers
    ])
    return props


def _declaring(properties, declared):
    """The caller's SUBSCRIBE properties with these declarations added.

    A copy, never the caller's own object, for the reason
    ``_with_headers`` is: a subscriber that builds one Properties and
    reuses it across calls would otherwise accumulate every slice it has
    ever declared, and be served the union of them.
    """
    if not declared:
        return properties
    props = copy.copy(properties) if properties is not None else Properties(
        PacketTypes.SUBSCRIBE
    )
    _set_user_properties(props, list(_user_properties(properties)) + list(declared))
    return props


def _set_user_properties(props, pairs):
    """Replace the User Properties, which is not what assignment does.

    ``UserProperty`` is one of the properties MQTT 5 permits more than
    once, and paho **appends** to a repeatable property on assignment
    rather than replacing it.  So writing the list a second time onto the
    same Properties leaves both - every header the caller sent twice
    over, and two ``saguin-id`` values on a record that may have one.
    Removing the attribute first is what makes the write a write.
    """
    if hasattr(props, "UserProperty"):
        del props.UserProperty
    props.UserProperty = list(pairs)


def _drain(waiting):
    """Everything in a queue right now, taken out so it can be sorted."""
    out = []
    while True:
        try:
            out.append(waiting.get_nowait())
        except queue.Empty:
            return out
