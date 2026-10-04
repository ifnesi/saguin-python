# saguin-python

**A Python client library for interacting with the
[Saguin](https://github.com/ifnesi/saguin) MQTT 5 broker** - a thin layer
over [Eclipse Paho](https://github.com/eclipse/paho.mqtt.python) that adds
what is saguin's and nothing else.

`saguin.Client` **subclasses** `paho.mqtt.client.Client`, and paho's own
methods are left as they are - the object *is* a paho client, so there is
no wrapper to escape from when this library has not grown the verb you
need. What it adds sits **beside** them, grouped by the kind of channel it
works on - `client.append`, `client.latest`, `client.queue` and
`client.admin`. The table of verbs is under *Working in channels* below,
in one place rather than two.

## Install

Not on PyPI yet - the name is reserved and a release waits on saguin's
own. From a checkout, in a virtual environment:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install .
```

Python 3.9 and up, which is what a long-term-support edge box still has.
The only dependency is `paho-mqtt>=2.0,<3`, pinned to a major because
paho 2.0 changed the callback signatures.

## Working in channels

The library adds saguin's verbs **beside** paho's, grouped by the kind of
channel they belong to. You name a channel and it works out the MQTT: it
asks the broker what the channel is, builds the topic from the channel's
filter and the key you gave, and puts a record id on every write.

```python
import saguin

client = saguin.Client("gateway-1")
client.start("broker.local", 1883)

client.append.publish("readings",
                      key=["site42", "device", "temp/1"],
                      value=b"21.5",
                      headers=[("unit", "C")])
```

| | |
|---|---|
| `client.append` | `.publish(channel, key, value, headers)`, `.consume(channel, key, start)`, `.seek(channel, to)` |
| `client.latest` | `.set(...)`, `.get(channel, key)`, `.delete(channel, key)`, `.consume(channel, key)` |
| `client.queue` | `.publish(...)`, `.fetch(channel)`, `.work(channel, handler)`, `.ack(job)`, `.nack(job)`, `.redrive(channel, record)` |
| `client.admin` | `.disconnect(client_id)` |

**Grouped rather than flat, because flat was three names for one act.**
Writing to an append channel, setting a key and queueing work are one
publish underneath, told apart only by which channel the topic lands in -
so a flat surface had to invent a different verb for each. Here the
channel type is the namespace and the verb is the same word.

It also keeps `client.admin.disconnect`, which hangs up somebody else,
well away from paho's `client.disconnect`, which hangs up you.

### The key fills in the channel's filter

One value per slot. For a channel filtered `iot/+/{device,sensor}/#`:

| filter level | `iot` | `+` | `{device,sensor}` | `#` |
|---|---|---|---|---|
| key | - | `site42` | `device` | `temp/1` |
| topic | `iot` | `site42` | `device` | `temp/1` |

A `+` takes any one level, a `{a,b}` level takes one of those spellings,
and a trailing `#` takes the rest - or nothing, since `#` stands for no
levels as well as for many. **A key that does not fit raises before
anything is sent, and says what the filter is:**

```
'gadget' is not one of 'device', 'sensor' at level '{device,sensor}'
  for channel 'readings', whose filter is 'iot/+/{device,sensor}/#'
```

There is no MQTT matcher in this library, and that is the point of
building the topic rather than checking it: a second matcher that
disagreed with the broker's would be worse than none.

**A verb on the wrong kind of channel is refused by name**, since the
three writes are one publish underneath and this is the only thing
between appending to a log and queueing work by mistake:

```
'tasks' is a queue channel, and this verb is for an append one
  - use client.queue.publish(), .fetch(), .work(), .ack(), .nack() or .redrive()
```

### Reading, and keeping your place

Any client can read. What needs `durable=True` is **keeping your place**:
coming back tomorrow and carrying on where you stopped, rather than at
whatever the channel says a reader with no position gets. `seek` needs it
too, since there is no position to move without one.

A durable client is three things together - clean start off, a session
expiry that is not zero, and the same id next time - and the broker stores
a position against all three. paho gets two of them wrong by default, in
the direction that loses the position.

```python
reader = saguin.Client("orders-reader", durable=True)
reader.start("broker.local", 1883)

for record in reader.append.consume("readings", key=["site42"]):
    handle(record)
```

The key narrows it. A value left out is every topic that level can hold -
and **a `{a,b}` level left out becomes one subscription per alternative
rather than a `+`**, because a `+` there would also reach topics beside
the channel, whose records are not its.

**A record is acknowledged when the loop asks for the next one**, not when
it arrives. The stored position advances on the acknowledgement, so
acknowledging on arrival would let a client that died half way through a
record resume *after* it - the one thing a position must never do. On an
append or `latest` channel, leaving the loop early leaves the record in
hand unacknowledged and it comes back: the position has not advanced, so
the next read is served it again.

**On a queue it does not come back until the client disconnects.** A job's
lease starts at the acknowledgement, so a job that was never acknowledged
has no lease to expire and nothing brings it round again - it is held for
a worker that has stopped asking, and the queue is one job short with
nothing to show for it. Acknowledge it or hand it back before leaving the
loop, which is what `queue.work` does on every path. Leave the loop
holding one and the library says so in a warning, because nothing else
would.

After `start`, `client.session_present` says whether the broker found your
session, and `client.granted_session_expiry` how long your position will
be kept - which is not always what you asked for, since the broker caps it.

**When the link drops**, paho reconnects on its own and a durable client's
subscription comes back with its session, so the loop carries on. The
record you had taken but not finished is sent again, because that is what
at-least-once means. **A client that keeps no place does not get its
subscription back** - there is no session for the broker to restore and
paho does not subscribe again by itself, so the loop goes quiet. That is
MQTT's own behaviour and `durable=True` is the answer to it.

### Where to begin, and where to jump

Both take **saguin's own vocabulary** and nothing invented: an integer -
`0` the retention floor, `-1` the next offset, or a position - or a string
holding a duration (`"12h"`, `"7d"`) or an RFC 3339 moment.

```python
reader.append.consume("readings", start=0)   # only if it has no position yet
reader.append.seek("readings", to="12h")     # always
```

**They are two verbs because they answer two questions.** `start` is where
to begin when this client has never read here, or its session expired -
and it fires **once**, because a value written into your code would
otherwise replay the whole channel on every restart. `seek` is a
deliberate move: replaying a day after a bug, or skipping a backlog.

The library knows which case it is in from `session_present`. One case it
cannot see: a session that exists but has never read *this* channel -
there is no verb for "where am I here", so the channel's own `start:`
setting in the broker's configuration decides that one.

**A bare integer always means an offset**, never a Unix time.
`1763000000` is a plausible offset and a plausible time, and seeking to
the wrong one reads on in order and reports success - so a time is a
string and the value is passed through unchanged.

### Taking a slice of a channel

Several readers can split one channel between them, each taking a share of
its topics, with **no coordination and no coordinator**. A reader says
which share is its own when it subscribes.

```python
for record in reader.append.consume("readings", key=["site42"],
                                    topic_hash=(8, 1)):
    handle(record)
```

That is member 1 of 8. The broker turns each topic into a number and
delivers a record only to the member whose share the remainder matches, and
**a reader declaring nothing gets everything** - which is every reader that
has never heard of this. It applies to a replay from a stored position as
well as to live records, and on a `latest` channel to the pass of current
state as well as to the changes after it.

Pass a list for a member holding more than one share -
`topic_hash=[(8, 1), (8, 5)]` - which is most often a member covering for a
peer that died. Every share in one call names the same total, because a
subscription has one partition space.

`saguin.partition(topic, count)` is the same calculation, on its own,
reaching no broker:

```python
saguin.partition("iot/site42/device/temp/1", 8)   # the member owed it
```

Use it rather than `saguin.topic_hash(topic) % 8`. The hash is only the
first half: RFC 0003 puts a mixing step after it, and without that step a
topic scheme carrying an identifier twice - `devices/<id>/msg/<id>` - sends
every identifier to a strict subset of the members and leaves the rest with
nothing. `topic_hash` is exposed because the RFC gives both halves so that
an implementation which disagrees can tell which one is wrong.

`partition` is here because **nothing tells you about a share nobody
claimed**. The broker cannot tell "there is no member 2" from "member 2
has not started yet", so it says nothing, and every reader that is running
looks healthy. Covering every share is the application's job by design,
and working out where your own topics fall is how it does it.

Three things worth knowing, all of them the broker's behaviour rather than
this library's:

* **A member's position moves past records outside its share**, so widening
  a share tomorrow recovers none of what it skipped yesterday.
* **A share lasts as long as the session** and survives a reconnection that
  resumes one.
* **Refused on a shared subscription and on a queue.** Both already divide
  a stream between their members; the queue is refused here, before
  anything is sent.

Broadcast takes it too, through paho's own subscribe, since a topic no
channel claims has no verb of its own:

```python
client.subscribe("shout/#", qos=1, topic_hash=(8, 1))
```

### Work

```python
worker = saguin.Client("packer", durable=True)
worker.start("broker.local", 1883)

for job in worker.queue.fetch("tasks"):
    try:
        do(job)
        worker.queue.ack(job)
    except Exception:
        worker.queue.nack(job)
```

`nack` sends the broker's own word, `return` - spelled `nack` here because
`return` is a Python keyword, and because ack/nack is the pair everybody
knows. The attempt is spent, and a job whose attempts run out is
dead-lettered.

Or let the library do the acking:

```python
worker.queue.work("tasks", pack_the_order)
```

A handler that returns normally has its job acked. One that raises has its
job handed back and **the worker carries on to the next** - the shape
RabbitMQ's clients and the frameworks over them have, because a worker
that stopped on one bad job would stop everything behind it. The failure
goes to the `saguin` log unless you pass `on_error=`.

A handed-back job comes straight back, so a handler that always raises
spends that job's attempts quickly. The queue's `max_attempts` bounds it
and the job is dead-lettered when they run out.

`fetch` and `work` take no key: a queue admits one subscription form and
no other, because two spellings would be two consumer groups each taking a
copy of every job.

### State

A `latest` channel keeps the current value of every key, so whoever
subscribes next is sent it - a device that was away learns the state
without anybody replaying a log at it.

```python
client.latest.set("state", key=["site42", "temp"], value=b"18")
client.latest.get("state", key=["site42", "temp"])      # b"18"
client.latest.delete("state", key=["site42", "temp"])
client.latest.get("state", key=["site42", "temp"])      # None
```

`get` answers `None` where there is no value - which is also what a
deleted key answers, since the two have always been the same here. It does
not subscribe, so reading a value once does not enrol you in every later
change to it.

To follow the state instead of asking for it:

```python
for record in reader.latest.consume("state", key=["site42"]):
    ...   # record.is_catch_up is True for the state you arrived to
```

### Hanging up a client

```python
client.admin.disconnect("device-7")     # "hung-up", or "no-such-client"
```

It ends the connection and leaves the session alone, so the device
reconnects and resumes at its stored position - the whole cost is one
reconnection. On its own it withdraws nothing: what the device may do when
it returns is whatever the broker's files say then. It needs a
`broker: sessions` rule in the broker's `acl_file`.

## Schemas

A schema registry needs nothing from the broker: a `latest` channel is
already a key-value store with delete, so a registry is **a channel and a
convention**. Register a schema by setting it, like any other value:

```python
client.latest.set("schemas", key=["acme/weather/v1"], value=AVRO_TEXT)
```

Then name **that topic** when you write; the library serializes for you:

```python
client = saguin.Client("gateway-1", schema_registry="schemas")

client.append.publish("readings", key=["site42", "device", "t"],
                      value={"site": "site42", "temp": 21.5},
                      schema="schemas/acme/weather/v1")
```

`schema=` works on every write - `append.publish`, `latest.set`,
`queue.publish` and plain `publish` - so it covers broadcast topics too.
It serializes the payload, sets the Content Type, and adds a `schema` User
Property carrying the schema's topic.

Reading it back:

```python
for record in reader.append.consume("readings"):
    record.schema     # "schemas/acme/weather/v1"
    record.deserialized    # {"site": "site42", "temp": 21.5}
```

**Avro and protobuf**, installed as extras, inside the same virtual environment -
`pip install 'saguin[avro]'` or `'saguin[protobuf]'`. The core stays paho-only: an edge box that
publishes bytes should not install a protobuf compiler to do it.

Four things worth knowing, each of which is a decision rather than an
accident:

* **The pointer is a whole topic, not an id.** A bare `weather-v1` lets
  two publishers in different domains pick the same name - the second
  silently replacing the first - and says nothing about where to look it
  up. A topic answers both.
* **There are no versions.** The topic is the identity, so **a new version
  is a new topic**. Rewriting a schema at the same topic changes what
  records already written against it mean, and nothing can recover the old
  text once it is gone. The library re-reads a schema whose text has
  changed; it cannot re-read one that no longer exists.
* **The schema must be registered before you produce.** There is no
  serializing from a local file, deliberately: a record written against a
  schema no consumer can fetch is a record nobody can read.
* **A pointer is followed only inside its own registry**, which is why the
  client is told where that is. An ACL governs who may *write* a topic and
  never who may *name* one, so a publisher could otherwise point a
  consumer at a `latest` channel holding device state and have it read out.

`record.deserialized` fetches the schema, so it needs the connection the record
was read on - deserialize inside the loop, not after it.

**Broadcast reads are the one path that hands you a paho message**, since
a topic no channel claims is ordinary MQTT. `client.record` turns one into
a saguin record:

```python
def on_message(client, userdata, msg):
    record = client.record(msg)
    print(record.deserialized)
```

**A schema is remembered for the life of the connection**, and republishing
at the same topic is not noticed. That is a limit rather than an oversight:
Avro's schemaless deserializing cannot reliably tell that it has the wrong
schema, so there is no failure to invalidate on, and an invalidation that
worked only for protobuf would be worse than none. It costs nothing where
the convention is followed - a new version is a new topic, and a new topic
is a cache miss. `client.forget_schema()` is there for anyone who
republishes at the same one anyway.

## What a delivery carries

```python
record.id           # the record's Message ID
record.offset       # its position in its channel, an int
record.timestamp    # broker receipt time, an aware UTC datetime
record.channel      # which channel - only where your filter reaches two
record.attempt      # which delivery of a queue job this is
record.is_catch_up  # state you are catching up on, not a change just made
record.headers      # the publisher's own User Properties
record.dlq          # why it failed out of a queue, or None
record.schema       # the topic of the schema it was written against
record.deserialized      # the payload read through that schema
record.paho         # the paho message, untouched
```

`record.headers` is a sequence rather than a dict, and deliberately: MQTT 5
permits a repeated name and it is the standard way to carry a list, so
`headers.all("tag")` answers every value and `headers["tag"]` the first.

Two names are worth reading twice. `record.timestamp` is **the broker's**
receipt time, not paho's local one - paho's is still on
`record.paho.timestamp`. And `record.channel` being `None` does not mean
"no channel": saguin sends the name only where your filter reaches more
than one, because a consumer whose filter reaches exactly one already knows
and would pay 26 bytes a message to be told.

## Dead letters

A dead-letter channel is an `append` channel in every other respect - its
name is the queue's with `__dlq` on the end - so it is read like any
other:

```python
for record in reader.append.consume("tasks__dlq"):
    ...
```

What is different is on the record:

```python
record.dlq.channel    # the queue it came from
record.dlq.reason     # attempts_exhausted, or expired
record.dlq.attempts   # how many were made
record.dlq.at         # when it was dead-lettered
```

A publisher cannot forge any of it: everything sent under the reserved
prefix is stripped, so this is the broker's own account.

### Putting failed work back

```python
for record in reader.append.consume("tasks__dlq"):
    if worth_retrying(record):
        worker.queue.redrive("tasks", record)
```

You name **the queue**, as you do everywhere else - a dead-letter channel
is the queue's own with `__dlq` on the end of its name, so there is
nothing to name twice.

This is a read and a republish done here, not a broker verb - deciding
that failed work should be tried again is a judgement nobody but you can
make. **The record keeps its own id**, so the work is the same work rather
than a second piece of it.

The `__dlq` level comes off **where the channel's filter puts it**, which
is not always the end: a queue filtered `bulk/#` has its dead letters at
`bulk/__dlq/...`. So the filter is asked for rather than assumed, and a
record whose topic does not carry `__dlq` there is refused - republishing
its topic unchanged would put the work straight back into the dead-letter
channel it came from.

**The dead letter stays where it is.** A dead-letter channel is an
`append` channel and reading one removes nothing, so redriving twice
queues the work twice; the id is what makes that noticeable.

## Plain MQTT is still there

`saguin.Client` **is** a paho client - `paho.mqtt.client.Client` is its
base class - so connecting, TLS, credentials, the network loop and
automatic reconnect are paho's and behave as they always have. `publish`
and `subscribe` are paho's own underneath, extended with the keyword
arguments described above and the two defaults below. A **broadcast**
topic, one no channel claims, is ordinary MQTT and is published to the
ordinary way.

```python
client.publish("iot/site42/hello", b"anyone there")
```

Two defaults differ from paho's, and both are saguin's answer rather than
a preference:

* **The protocol is MQTT 5** and nothing else is accepted. 3.1.1 has no
  User Properties, no Response Topic, no shared subscriptions and no
  session expiry - which is everything this library adds. (The broker
  itself serves 3.1.1 clients; this library does not speak for them.)
* **A publish is QoS 1** unless you ask for another. Saguin answers an
  ordinary publish refusal on the PUBACK, and at QoS 0 there is no PUBACK,
  so every one of those arrives as silence. (The refusals that close the
  connection instead do so at any QoS.)

Every write carries a `saguin-id` - a UUIDv7 the broker stores as the
record's Message ID, stable across redelivery, dead-lettering and replay,
and what a consumer deduplicates on. **The library handles it**: supply
your own with `saguin_id=` and it is used unchanged, which is what makes a
retry the same record rather than a second one; supply none and one is
minted. Either way the id that went out is on the object returned.

**Three callbacks have a handler of the library's in front of them** -
`on_connect`, `on_subscribe` and `on_publish` - because each carries an
answer the library needs: what session expiry was granted and whether the
session was found, which filters a SUBACK refused, and whether a question
was refused and in what words. Assigning works as it does on a paho
client: yours is stored and called after the library's. Reading one
answers the library's handler rather than yours, which is what makes paho
call it.

**`on_message` has one in front of it too, and it is worth knowing why.**
While a `consume` loop is reading a filter, records matching it go to
that reader - paho calls a topic callback *instead of* `on_message`, not
as well as it. Everything else reaches your handler, which is what makes
broadcast work:

```python
client.on_message = my_handler       # broadcast, and anything unread
```

**Nothing is acknowledged except by whoever reads it**, and that includes
your handler. This client is built with paho's `manual_ack`, deliberately:
a channel record's acknowledgement is what moves the stored position, so
it must not go out before the record has been read. A `consume` loop
acknowledges what it hands you when you ask for the next record. What
reaches `on_message` is yours to answer for, with paho's own verb:

```python
def my_handler(client, userdata, msg):
    handle(msg)
    client.ack(msg.mid, msg.qos)     # broadcast, and anything unread
```

**A record you never acknowledge is one the broker still holds.** That is
the point of it on a durable session - an unacknowledged QoS 1 delivery is
sent again on the next connection, and for a broadcast topic that
redelivery is the only replay there is, since no position is stored behind
it. It is also the cost: acknowledge nothing for long enough and the
broker's window of records in flight to you fills, and it stops sending.
`saguin.Client(..., manual_ack=False)` hands the job back to paho, which
answers every delivery on arrival, and is the right answer only where
nothing this client reads keeps a position.

**A handler that raises stops the reading**, and quietly: the exception
escapes into paho's network thread, which ends, while `is_connected()`
goes on answering True. That is paho's own behaviour rather than this
library's, and the remedy is the ordinary one - catch what your handler
can raise. With manual acknowledgement the record it was holding stays
unacknowledged, so it survives for the next connection.

**And what reaches nobody is kept rather than dropped.** A durable client
that reconnects is served records for subscriptions made on its previous
connection, before your code has called `consume` for them; `consume`
sweeps those out when it starts. The rest wait, bounded at ten thousand,
and the library says so on the `saguin` logger rather than losing them
quietly - a client that neither consumes nor sets `on_message` would
otherwise grow a queue nobody empties.

**Reading a channel twice on one client takes it over.** paho keys a topic
callback by its filter, so two readers of one filter cannot both hold it -
read, seek, read again is ordinary, so the second wins and the first is
closed, keeping whatever it was still holding.

**Let go of a reader when you have finished with it.** It holds its
subscription until its loop ends or it is closed: a `for` that runs out
closes itself, and a bare `next(...)` does not. On a queue that matters
more than it looks - an abandoned reader stays in the consumer group and
goes on being handed work nobody is reading.

```python
records = client.queue.fetch("tasks")
job = next(records)
records.close()          # or take them in a loop that ends
```

## Status

Early. What is here: `client.append`, `client.latest`, `client.queue` -
including a callback worker and putting dead letters back -
`client.admin.disconnect`, and schemas for Avro and protobuf.

**The operations listener is deliberately not here.** Its routes -
sessions, config, the ACL, the metrics - take the *operator's* credential,
and putting that in the library a fleet installs is the one shape worth
not offering: a fleet's credentials live on the fleet, where anyone
holding one device can read them. It is eight GETs with Basic auth and
JSON for anyone who needs them, and `saguin-viewer` is the worked example.

**Nothing here decides what a broker will accept.** The broker is the
enforcement point and refusals come back as its own reason code and
sentence, raised verbatim. What this library does check is its own
contract - that a key fits the filter it is being composed against, and
that a schema pointer stays inside its registry. Neither is authorization:
the first is arithmetic on levels, and the second is a rule the broker
has no opinion about, since it never reads a `schema` property.

## A guided tour

`examples/demo.py` runs the whole of this against a broker it starts
itself - every verb, and then a section that deliberately breaks things,
because what you need from a tour is not only the working call but what
comes back when it is wrong.

```sh
SAGUIN_BROKER=/path/to/saguin python examples/demo.py
```

It pauses between sections; `SAGUIN_DEMO_PAUSE=0` runs it straight
through. The suite runs it too, so it cannot quietly stop matching the
library.

## Testing

The suite drives a **real broker** rather than a mock - what is being
tested is what saguin does with what a client sent, and a mock would
answer with what this library believes saguin does. Until saguin
publishes a release binary, point the suite at one you built (`make build`
in a saguin checkout writes `./bin/saguin`):

```sh
make install                              # a .venv with the library and pytest
SAGUIN_BROKER=/path/to/saguin make test
```

`make test` uses `.venv`; pass `PYTHON=` to point it at another
interpreter.

The suite writes its own configuration and starts the broker on a free
port. Where a test is about **what the broker put on the wire**, it reads
the record back with plain paho rather than with this library - a probe
that asks the code under test what the answer should be asserts only that
the code agrees with itself. Where a test is about **the library's own
verbs**, it uses them, because that is what is under test.

## Contributing

Contributions are welcome. The most useful one is finding a claim here
that the broker does not keep. [CONTRIBUTING.md](CONTRIBUTING.md) has the
rules, and there are only two unusual ones: nothing in this library may be
a rule the broker does not enforce, and a new test is watched failing
before it is trusted.

## Development approach

**saguin-python is written by Claude.**

## Licence

Apache 2.0, the same as saguin.
