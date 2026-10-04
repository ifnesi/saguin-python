"""The verbs, grouped by the kind of channel they belong to.

``client.append.publish(...)``, ``client.latest.get(...)``,
``client.queue.fetch(...)``, ``client.admin.disconnect(...)``.

**Grouped rather than flat, because the flat version was three names for
one act.** Writing to an append channel, setting a key and queueing work
are one publish underneath, told apart only by which channel the topic
lands in - so a flat surface had to invent a different verb for each, and
a reader had to know which invented word went with which channel type.
Here the channel type is the namespace and the verb is the same word.

It also keeps `client.admin.disconnect`, which hangs up somebody else,
away from paho's own `client.disconnect`, which hangs up you.
"""

import logging

# saguin's own, and not configurable: a queue's dead letters are the
# channel of the same name with this on the end, and the level appears in
# that channel's filter where the queue's filter had its `#`, or last.
DLQ_LEVEL = "__dlq"
DLQ_SUFFIX = "__dlq"

# Named for the library, so an application can turn this up or down
# without touching its own logging.
_log = logging.getLogger("saguin")


class Verbs:
    """What every group shares: the client, and the type it works on."""

    __slots__ = ("_client",)
    kind = None

    def __init__(self, client):
        self._client = client

    def _channel(self, name, timeout=10):
        info = self._client.channel(name, timeout=timeout)
        self._client._must_be(info, self.kind)
        return info


class Append(Verbs):
    """A durable, replayable stream of records."""

    __slots__ = ()
    kind = "append"

    def publish(self, channel, key=(), value=None, headers=None, **kw):
        """Add a record. Consumers keep their own positions, so nothing
        another reader has seen is consumed away from anybody else."""
        return self._client._write(self.kind, channel, key, value, headers, **kw)

    def consume(self, channel, key=(), start=None, **kw):
        """Read the channel from where this client left off, yielding
        records.

        ``start`` says where to begin **when this client has no position
        yet** - it has never read here, or its session expired. It is the
        broker's own vocabulary: an integer offset (`0` the retention
        floor, `-1` the next offset, or a position), or a string holding a
        duration such as `"12h"` or `"7d"`, or an RFC 3339 moment.

        It applies once rather than on every call, which is the whole
        reason it is not a seek: a value written once in the code would
        otherwise replay the channel on every restart. To move a position
        deliberately, `seek`.

        ``topic_hash=(partitions, index)`` takes a slice of what this
        reaches, so that several members can split a channel between them
        with no coordination and per-topic order kept inside each. It
        applies to the replay from a stored position as well as to live
        records, and a member's position moves past the records outside
        its slice - so widening the slice later recovers none of them.
        """
        return self._client._consume(self.kind, channel, key, start=start, **kw)

    def seek(self, channel, to, timeout=10):
        """Move this client's position, and answer the offset it landed on.

        Takes the same values as ``consume``'s ``start`` and applies them
        **always**: replaying a day after a bug, or skipping a backlog, is
        a deliberate act rather than a default.
        """
        return self._client._seek(channel, to, timeout=timeout)


class Latest(Verbs):
    """Current value per topic: a key-value store that survives restart.

    saguin calls this a `latest` channel, which is the word in the
    configuration file and in the documents, so it is the word here.
    """

    __slots__ = ()
    kind = "latest"

    def set(self, channel, key=(), value=None, headers=None, **kw):
        """Set the value of one key. Whoever subscribes next is sent it,
        so a device that was away learns the state."""
        return self._client._write(self.kind, channel, key, value, headers, **kw)

    def get(self, channel, key=(), timeout=10):
        """Read one key's current value, or **None where there is none**.

        A key never set and one that was deleted are the same answer, as
        they are everywhere else on this channel type. It does not
        subscribe: reading a value once does not enrol you in every later
        change to it.
        """
        return self._client._point_read(self._channel(channel, timeout), key, timeout)

    def delete(self, channel, key=(), headers=None, **kw):
        """Remove one key.

        It is a write with nothing in it, which is how MQTT already says
        "gone" - and it has a name here because "publish an empty payload
        to delete it" is exactly the lore this library exists to remove.
        """
        return self._client._write(self.kind, channel, key, b"", headers, **kw)

    def consume(self, channel, key=(), **kw):
        """Yield the current value of everything this reaches, then every
        change. ``record.is_catch_up`` tells the two apart.

        ``topic_hash=(partitions, index)`` takes a slice, and takes it on
        **both** halves: a member sent the whole of current state and then
        only its share of the changes would hold a copy that starts
        complete and drifts."""
        return self._client._consume(self.kind, channel, key, **kw)


class Queue(Verbs):
    """Work handed to one worker at a time."""

    __slots__ = ()
    kind = "queue"

    def publish(self, channel, key=(), value=None, headers=None, **kw):
        """Put work on the queue, for one worker to take."""
        return self._client._write(self.kind, channel, key, value, headers, **kw)

    def fetch(self, channel, **kw):
        """Take work, yielding one job at a time.

        Not narrowed by a key: a queue admits one subscription form and no
        other, because two spellings would be two consumer groups each
        taking its own copy of every job.
        """
        return self._client._consume(self.kind, channel, (), **kw)

    def work(self, channel, handler, on_error=None, **kw):
        """Take work and hand each job to ``handler``, until nothing comes.

            worker.queue.work("tasks", pack_the_order)

        A handler that returns normally has its job **acked** - resolved,
        and nobody else will see it. A handler that raises has its job
        **nacked** - handed back to be offered again now - and the worker
        **carries on to the next job**.

        That is the shape RabbitMQ's clients and the frameworks over them
        have: log the failure and keep going. A worker that stopped on one
        bad job would stop processing everything behind it, which is a
        worse outcome than the job that failed.

        **The failure is written to a log rather than swallowed.** A queue
        that fails every job with nothing anywhere saying so is the thing
        this is written to avoid. Pass ``on_error(job, exception)`` to do
        something else with it instead.

        **A handed-back job comes straight back**, to this worker, since
        it is the one asking - so a handler that always raises for the
        same job will spend its attempts in quick succession. That is not
        a runaway to guard against here: the queue's `max_attempts` bounds
        it and the job is dead-lettered when they run out, which is the
        broker's answer to work that cannot be done.

        Returns the number of jobs handled - offers rather than distinct
        jobs, since one job handed back and taken again is two - so a
        caller can tell "the queue was empty" from "something happened".
        """
        done = 0
        for job in self.fetch(channel, **kw):
            try:
                handler(job)
            except Exception as failed:  # noqa: BLE001 - a worker outlives its jobs
                self.nack(job)
                if on_error is not None:
                    on_error(job, failed)
                else:
                    _log.exception(
                        "handing back a job from %r that raised: %s", channel, failed
                    )
            else:
                self.ack(job)
            done += 1
        return done

    def redrive(self, channel, record, timeout=10):
        """Put one dead-lettered record back on its queue.

            for record in reader.append.consume("tasks__dlq"):
                if worth_retrying(record):
                    worker.queue.redrive("tasks", record)

        ``channel`` is **the queue**, which is what you name everywhere
        else: a dead-letter channel is the queue's own, with `__dlq` on
        the end of its name, so there is nothing to name twice.

        **Not a broker verb**: it is a read and a republish done here,
        because deciding that failed work should be tried again is a
        judgement nobody but the operator can make.

        The `__dlq` level comes off **where the dead-letter channel's own
        filter puts it**, which is not always the end - a queue filtered
        `bulk/#` has its dead letters at `bulk/__dlq/...`. So the filter
        is asked for rather than assumed, and a record whose topic does
        not carry `__dlq` there is refused: it is not a record this rule
        describes, and republishing its topic unchanged would put the work
        straight back into the dead-letter channel it came from.

        **The record's own id travels with it**, so the work keeps the
        identity a consumer deduplicates on rather than becoming a second
        piece of work. Everything else the publisher sent goes unchanged,
        and the broker's own dead-letter account does not: a client may
        not write under the reserved prefix.

        **The dead letter stays where it is.** A dead-letter channel is an
        `append` channel and reading one removes nothing, so redriving
        twice queues the work twice - the id is what makes that something
        a consumer can notice.
        """
        # The queue first, through this namespace's own check: naming
        # anything else here is the mistake worth catching, and it is
        # caught before the record is looked at.
        self._channel(channel, timeout)

        dead_letters = self._client.channel(channel + DLQ_SUFFIX, timeout=timeout)
        levels = dead_letters.filter.split("/")
        try:
            at = levels.index(DLQ_LEVEL)
        except ValueError:
            raise ValueError(
                "{!r} holds no {!r} level, so this broker does not put "
                "{!r}'s dead letters where this expects".format(
                    dead_letters.filter, DLQ_LEVEL, channel
                )
            )

        topic = record.topic.split("/")
        if len(topic) <= at or topic[at] != DLQ_LEVEL:
            raise ValueError(
                "{!r} does not carry {!r} at level {} where {!r} puts it, so it "
                "is not one of {!r}'s dead letters".format(
                    record.topic, DLQ_LEVEL, at + 1, dead_letters.filter, channel
                )
            )

        return self._client.publish(
            "/".join(topic[:at] + topic[at + 1:]),
            payload=record.payload,
            headers=record.headers.pairs,
            saguin_id=record.id,
        )

    def ack(self, job):
        """The work succeeded: resolve the record.

        Answered on the connection the job arrived on, because the broker
        takes a job's answer only from the session holding it.
        """
        return self._client._answer_job(job, b"ack")

    def nack(self, job):
        """The work failed: hand it back to be offered again now.

        Sends the broker's own word, ``return`` - spelled ``nack`` here
        because ``return`` is a Python keyword and because ack/nack is the
        pair everybody already knows. The attempt is spent, and a job
        whose attempts run out is dead-lettered with ``record.dlq`` saying
        why.
        """
        return self._client._answer_job(job, b"return")


class Admin(Verbs):
    """What an ordinary MQTT connection may ask the broker to do.

    Not the operations listener - sessions, config, the ACL and the
    metrics are a different door with the **operator's** credential, and
    that stays a separate object, because an operator's credential must
    not be a device's.
    """

    __slots__ = ()

    def disconnect(self, client_id, timeout=10):
        """Hang up a connected client, by id.

        It ends the connection and leaves the session alone, so the device
        reconnects and resumes at its stored position - the whole cost is
        one reconnection. On its own it withdraws nothing: what the device
        may do when it returns is whatever the broker's files say then.

        Answers ``"hung-up"`` or ``"no-such-client"``, and the two are
        worth keeping apart: otherwise "that device went away an hour ago"
        and "you have misspelled the id" read the same.
        """
        return self._client._disconnect_client(client_id, timeout)
