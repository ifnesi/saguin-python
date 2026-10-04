"""Turning a channel's filter into the topic to publish on, and back.

A saguin channel is a name and an MQTT topic filter, and the filter is the
one thing a client cannot work out for itself - it lives in the operator's
configuration.  Once the broker has told you the filter, a topic in that
channel is not something to validate but something to **build**: every
level of the filter is either spelled out, or a slot you fill.

That is why there is no MQTT matcher in this library.  A composed topic
matches the filter because it was made from it, so there is nothing to
check afterwards and nothing that could disagree with the broker about
what matches what.

Four kinds of level, and only the last three take an argument:

    iot                 spelled out - nothing to supply
    +                   any one level, so no "/" in what you give it
    {device,sensor}     one of these exactly
    #                   the rest, or nothing; always last if present
"""


class ChannelInfo:
    """What the broker says a channel is.

    ``filter`` is as the operator wrote it, braces included, because that
    is what says which levels take an argument and what each will accept.
    """

    __slots__ = ("name", "type", "filter", "verbs", "pin")

    def __init__(self, name, type, filter, verbs=(), pin=None):
        self.name = name
        self.type = type
        """``append``, ``latest`` or ``queue``."""
        self.filter = filter
        self.verbs = tuple(verbs)
        """What this client may do here, as the broker answered when
        asked.  A permission is still checked when it is used - an
        operator may withdraw one while you are connected."""
        self.pin = pin
        """A queue's subscription form.  Only a queue has one, and it is
        the only form a queue admits."""

    @classmethod
    def from_json(cls, data):
        return cls(
            name=data["name"],
            type=data["type"],
            filter=data["filter"],
            verbs=data.get("verbs", ()),
            pin=data.get("pin"),
        )

    def slots(self):
        """The levels of this filter that take an argument, in order.

        Each is ``None`` for a ``+``, a tuple of the permitted spellings
        for a braced level, or the string ``"#"`` for a trailing ``#``.
        """
        found = []
        for level in self.filter.split("/"):
            if level == "+":
                found.append(None)
            elif level == "#":
                found.append("#")
            elif level.startswith("{") and level.endswith("}"):
                found.append(tuple(level[1:-1].split(",")))
        return found

    def __repr__(self):
        return "ChannelInfo(name={!r}, type={!r}, filter={!r})".format(
            self.name, self.type, self.filter
        )


class KeyDoesNotFit(ValueError):
    """The key given does not fit the channel's filter.

    **Every one of these names the filter**, because the person reading it
    is writing a client against a channel somebody else configured, and
    the filter is the whole of what they need to know to fix it.
    """

    def __init__(self, info, said):
        self.channel = info.name
        self.filter = info.filter
        super().__init__(
            "{} for channel {!r}, whose filter is {!r}".format(said, info.name, info.filter)
        )


def compose(info, key=()):
    """The topic to publish on, built from the filter and the key.

    ``key`` supplies one value per argument-taking level, in order.  A
    trailing ``#`` may be left out, since ``#`` stands for no levels as
    well as for many.
    """
    key = _as_list(info, key)
    slots = info.slots()
    trailing_hash = bool(slots) and slots[-1] == "#"

    least = len(slots) - 1 if trailing_hash else len(slots)
    if len(key) < least or len(key) > len(slots):
        raise KeyDoesNotFit(
            info,
            "the key has {} {} and the filter takes {}".format(
                len(key), "value" if len(key) == 1 else "values", _expected(slots)
            ),
        )

    out, given = [], list(key)
    for level in info.filter.split("/"):
        if level == "+":
            out.append(_one_level(info, given.pop(0), level))
        elif level == "#":
            if given:
                rest = _no_wildcards(info, given.pop(0), level)
                if rest:
                    out.append(rest)
        elif level.startswith("{") and level.endswith("}"):
            out.append(_alternative(info, given.pop(0), level))
        else:
            out.append(level)
    return "/".join(out)


def subscriptions(info, key=()):
    """The topic filters to subscribe to, narrowed by whatever key was given.

    A value left out is every topic that level can hold: ``+`` for a ``+``
    level and ``#`` for a trailing ``#``.  **A braced level left out
    becomes one filter per alternative rather than a ``+``** - a ``+``
    there would also reach topics beside the channel, which this channel
    does not claim and whose records are not its.
    """
    key = _as_list(info, key)
    slots = info.slots()
    if len(key) > len(slots):
        raise KeyDoesNotFit(
            info,
            "the key has {} values and the filter takes {}".format(len(key), _expected(slots)),
        )

    filters, given = [""], list(key)
    for level in info.filter.split("/"):
        if level == "+":
            chosen = [_one_level(info, given.pop(0), level)] if given else ["+"]
        elif level == "#":
            chosen = [_no_wildcards(info, given.pop(0), level) or "#"] if given else ["#"]
        elif level.startswith("{") and level.endswith("}"):
            alternatives = tuple(level[1:-1].split(","))
            chosen = [_alternative(info, given.pop(0), level)] if given else list(alternatives)
        else:
            chosen = [level]
        filters = [
            (prefix + "/" + one) if prefix else one for prefix in filters for one in chosen
        ]
    return filters


def _as_list(info, key):
    if key is None:
        return []
    if isinstance(key, str):
        # One value spelled without a list is the common case and reads
        # well; the split that would otherwise happen over its characters
        # is the kind of quiet wrong answer this library exists to avoid.
        return [key]
    return list(key)


def _expected(slots):
    if not slots:
        return "none"
    said = []
    for one in slots:
        if one is None:
            said.append("any one level")
        elif one == "#":
            said.append("the rest, or nothing")
        else:
            said.append("one of " + ", ".join(repr(a) for a in one))
    return "{}: {}".format(len(slots), "; ".join(said))


def _one_level(info, value, level):
    value = str(value)
    if "/" in value:
        raise KeyDoesNotFit(
            info, "{!r} holds a '/' and {!r} is one level".format(value, level)
        )
    return _no_wildcards(info, value, level)


def _alternative(info, value, level):
    value = str(value)
    alternatives = tuple(level[1:-1].split(","))
    if value not in alternatives:
        raise KeyDoesNotFit(
            info,
            "{!r} is not one of {} at level {!r}".format(
                value, ", ".join(repr(a) for a in alternatives), level
            ),
        )
    return value


def _no_wildcards(info, value, level):
    value = str(value)
    if "+" in value or "#" in value:
        raise KeyDoesNotFit(
            info,
            "{!r} holds a wildcard at level {!r}, and MQTT allows none in a "
            "topic published to".format(value, level),
        )
    return value


def expand(filter):
    """The plain filters a written one stands for, resolving `{a,b}` levels.

    The broker does this before it matches, so a check made here against
    the written form would answer about a filter nothing uses.
    """
    out = [""]
    for level in filter.split("/"):
        if level.startswith("{") and level.endswith("}"):
            alternatives = level[1:-1].split(",")
        else:
            alternatives = [level]
        out = [
            (prefix + "/" + one) if prefix else one
            for prefix in out
            for one in alternatives
        ]
    return out


def inside(filter, topic):
    """Whether a topic lands inside a filter - ordinary MQTT matching.

    **The one place this library matches rather than composes**, and it is
    here because nothing else can do the job: a consumer must not follow a
    schema pointer out of its registry, since an ACL governs who may
    *write* a topic and never who may *name* one - so a publisher can
    point at a `latest` channel holding device state and have a consumer
    read it out.

    Nothing competes with the broker here. The broker has no opinion about
    schema pointers at all, and getting this wrong fails safe: a pointer
    is refused rather than data misrouted.
    """
    parts = topic.split("/")
    for one in expand(filter):
        levels = one.split("/")
        for i, level in enumerate(levels):
            if level == "#":
                if i <= len(parts):  # `#` stands in for nothing as well
                    return True
                break
            if i >= len(parts) or (level != "+" and level != parts[i]):
                break
        else:
            if len(levels) == len(parts):
                return True
    return False


# -- taking a slice of what a filter reaches --------------------------------

# The one User Property a subscriber declares itself with.  It is on the
# packet rather than on a filter, so one declaration applies to every
# filter in that SUBSCRIBE - and repeating the property is an OR, which is
# how a member holds more than one slice.
DECLARATION = "saguin-filter"

# 2147483647, and it is not a performance limit: it is the largest number
# a 32-bit gateway and a 64-bit server agree on, so the same SUBSCRIBE is
# legal on both.
MAX_PARTITIONS = 2147483647


def topic_hash(topic):
    """FNV-1a, 64-bit, over the topic's UTF-8 bytes.

    The first half of what a slice is taken from, and on its own it is
    **not** the answer to "which member holds this topic" - ``partition``
    is. It is exposed because RFC 0003 states the two halves separately so
    that an implementation which disagrees can tell which of them is wrong.

    The bytes are the topic's UTF-8 bytes and not its characters, and the
    multiplication wraps at 64 bits.
    """
    value = 14695981039346656037
    for byte in topic.encode("utf-8"):
        value ^= byte
        value = (value * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return value


def partition(topic, partitions):
    """Which slice of ``partitions`` holds ``topic``: 0 to partitions - 1.

    **A constant of the protocol rather than an implementation detail**,
    which is why it is here rather than only in the broker: a consumer can
    work out which of its own topics are its own share without asking
    anybody, and a test can say which member should receive a record
    rather than accepting whichever one did.

    Nothing reports a share nobody claimed - the broker cannot tell that
    from a member which has not started - so covering every share is the
    application's job, and this is what it does it with.

    **The mixing step is not optional and not this library's invention.**
    FNV-1a stirs the top of its accumulator and hardly touches the bottom,
    which is the half a modulus reads, so a topic scheme carrying an
    identifier twice - `devices/<id>/messages/devicebound/<id>` - cancels
    that identifier out of the bottom and sends every one of them to a
    strict subset of the shares. Every modern hash ends with a step like
    this; FNV-1a is the unusual one for stopping early.
    """
    if not isinstance(partitions, int) or isinstance(partitions, bool) or \
            partitions < 1:
        raise ValueError(
            "partitions is a whole number of 1 or more, and {!r} is not".format(
                partitions
            )
        )
    value = topic_hash(topic)
    value ^= value >> 30
    value = (value * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 27
    value = (value * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 31
    return value % partitions


def declarations(topic_hash=None, saguin_filter=None):
    """The ``saguin-filter`` User Properties a SUBSCRIBE should carry.

    ``topic_hash`` is a ``(partitions, index)`` pair - or several of them
    for a member holding several slices, which are an OR.  The argument is
    named for the function that goes on the wire, so a second function
    arrives here as a second argument rather than as a larger language.

    ``saguin_filter`` is the door for a function this library has not
    heard of: its strings go on the packet unchanged, so a broker that has
    grown one is usable from a client that predates it.

    **What is checked here is what cannot depend on the broker**: a count
    outside its bound, an index at or above its count, and two calls
    naming different counts.  Each is refused by every saguin there will
    ever be, so refusing them here turns a `0x83` arriving later on a
    SUBACK - after the caller has a generator in hand - into a sentence at
    the call that caused it.  Everything else is the broker's to answer.
    """
    # The parameter shadows the function of the same name above, which is
    # deliberate: the keyword a caller types is the name of the call it
    # builds.  Nothing in here needs the hash.
    out, agreed = [], None
    for partitions, index in _slices(topic_hash):
        if agreed is None:
            agreed = partitions
        elif partitions != agreed:
            raise ValueError(
                "every slice on one subscribe must name the same number of "
                "partitions, and {} is not {} - a subscription has one "
                "partition space, and mixed ones are refused".format(
                    partitions, agreed
                )
            )
        out.append((DECLARATION, "topic_hash({}, {})".format(partitions, index)))
    for one in _raw(saguin_filter):
        out.append((DECLARATION, one))
    return out


def _slices(declared):
    """``(8, 1)`` or ``[(8, 1), (8, 5)]``, checked, as a list of pairs."""
    if declared is None:
        return []
    pairs = list(declared)
    if not pairs:
        # **An empty collection is refused, and `None` is the way to
        # declare nothing.** RFC 0003 refuses an empty `saguin-filter`
        # value rather than ignoring it, because a client that asked for a
        # share and was quietly served the whole channel has nowhere to
        # notice - and the same reasoning reaches an empty list. The
        # caller most likely to pass one is a member whose computed share
        # came out empty by mistake, and every such member would process
        # everything.
        raise ValueError(
            "topic_hash is empty, which would declare nothing and be served "
            "the whole channel - pass None, or leave it out, to ask for "
            "everything on purpose"
        )
    if _is_whole(pairs[0]):
        # One pair written without a list, which is the common case.
        pairs = [declared]
    return [_slice(one) for one in pairs]


def _slice(one):
    try:
        pair = list(one)
    except TypeError:
        # **Reached by a pair of the wrong kind**, `(True, False)` being
        # the one somebody writes: `_is_whole` refuses a bool, so the pair
        # is taken for a list of pairs and each half for a pair of its own.
        # Without this the caller gets the interpreter's sentence, and
        # every other wrong shape in this module gets one naming the
        # mistake.
        raise ValueError(
            "a slice is a (partitions, index) pair of whole numbers, and "
            "{!r} is not".format(one)
        ) from None
    if len(pair) != 2 or not all(_is_whole(n) for n in pair):
        raise ValueError(
            "a slice is a (partitions, index) pair of whole numbers, and "
            "{!r} is not".format(one)
        )
    partitions, index = pair
    if not 1 <= partitions <= MAX_PARTITIONS:
        raise ValueError(
            "{} partitions is outside 1 to {} - a count of 1 is legal and "
            "means one slice holding everything".format(partitions, MAX_PARTITIONS)
        )
    if not 0 <= index < partitions:
        # **The check that makes a swapped pair safe.** An index must be
        # below its count, so if (8, 1) is a slice then (1, 8) cannot be -
        # which holds for every valid pair, so arguments given the wrong
        # way round are always refused rather than quietly serving a
        # slice nobody asked for.
        raise ValueError(
            "index {} is not below the {} partitions it is an index into - "
            "an index runs 0 to {}, so a (partitions, index) pair given the "
            "other way round is refused here".format(
                index, partitions, partitions - 1
            )
        )
    return partitions, index


def _raw(declared):
    if declared is None:
        return []
    values = [declared] if isinstance(declared, str) else list(declared)
    if not values:
        # The same rule as an empty topic_hash, for the same reason.
        raise ValueError(
            "saguin_filter is empty, which would declare nothing and be "
            "served the whole channel - pass None, or leave it out, to ask "
            "for everything on purpose"
        )
    for one in values:
        if not isinstance(one, str) or not one:
            # An empty value is refused rather than dropped: a client that
            # asked for a slice and was served the whole channel has
            # nowhere to notice it, which is what the broker refuses it
            # for as well.
            raise ValueError(
                "a saguin-filter value is a non-empty call such as "
                "'topic_hash(8, 1)', and {!r} is not".format(one)
            )
    return values


def _is_whole(value):
    # `bool` is an `int` in Python, and `topic_hash(True, False)` is not
    # something anybody meant.
    return isinstance(value, int) and not isinstance(value, bool)
