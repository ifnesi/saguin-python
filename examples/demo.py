#!/usr/bin/env python3
"""A guided tour of the saguin Python library, against a real broker.

    SAGUIN_BROKER=/path/to/saguin python examples/demo.py

It starts a broker of its own, walks through every verb the library has,
and then goes out of its way to **break things**, because what a developer
needs from a tour is not only the shape of the working call - it is what
comes back when the call is wrong, and whether that tells them enough to
fix it.

Nothing here is set up behind your back: the broker's configuration is
printed, the channels are the ones in it, and every call in the tour is
one you could type.
"""

import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))
# **The tour borrows the suite's broker rather than starting one its own
# way.** Both had a copy of "where is the binary, and how do I wait for
# it to listen", with two different refusals when it was missing - one
# rule in two places, which is how the two drift apart.
sys.path.insert(0, os.path.join(HERE, "..", "tests"))

import saguin  # noqa: E402
from broker import Broker, broker_binary  # noqa: E402
from saguin import schemas  # noqa: E402

BOLD, DIM, RED, GREEN, YELLOW, OFF = (
    "\033[1m", "\033[2m", "\033[31m", "\033[32m", "\033[33m", "\033[0m")
HOST = "127.0.0.1"
# Pausing needs somebody to press the key. Piped or redirected, there is
# nobody, and waiting for one is a crash rather than a pause.
PAUSE = os.environ.get("SAGUIN_DEMO_PAUSE", "1") != "0" and sys.stdin.isatty()


def say(text):
    print("\n{}== {}{}".format(BOLD, text, OFF))


def note(text=""):
    print("   {}{}{}".format(DIM, text, OFF))


def code(text):
    for line in text.strip("\n").splitlines():
        print("   {}{}{}".format(GREEN, line, OFF))


def did(text):
    """Something the tour just did, said out loud.

    **Every action is announced**, because a tour that publishes quietly
    and then shows three records where you counted one is teaching you
    nothing - it is asking you to trust it. Anybody presenting this, or
    reading it to learn the library, has to be able to account for every
    record on the screen.
    """
    print("   {}-> {}{}".format(YELLOW, text, OFF))


def shown(label, value):
    print("   {:<22} {}".format(label, value))


def broke(exception):
    """What came back when the call was wrong. The whole point of the last
    section, and worth reading in full: an error that does not say what to
    do next is a defect in this library."""
    print("   {}{}: {}{}".format(RED, type(exception).__name__, exception, OFF))


def one(records):
    """The next record, and then let go of the reader.

    **Worth a helper because abandoning one is a trap.** A reader holds
    its subscription until its loop ends or it is closed, so
    `next(client.queue.fetch(...))` takes one job and leaves you in the
    consumer group - still being handed work that nobody is reading. A
    `for` loop that runs out closes itself; a bare `next` does not.
    """
    try:
        return next(records)
    finally:
        records.close()


def wait():
    if not PAUSE:
        return
    try:
        input("\n   {}[ENTER] to continue{}".format(YELLOW, OFF))
    except EOFError:  # somebody closed the input; walk the rest of it
        globals()["PAUSE"] = False


# The broker's configuration is examples/saguin.yaml - a file you can
# read, edit and start a broker with yourself. The tour copies it and
# replaces the address with a free port, so it cannot collide with a
# broker you already have running; nothing else about it changes.
CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saguin.yaml")


# The schemas are files under examples/schemas/, and the tree is the
# registry's layout: examples/schemas/acme/weather/v1.avsc is registered
# at the topic schemas/acme/weather/v1. Nothing here is hard-coded - add a
# file and the tour registers it.
SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas")


def schemas_on_disk():
    """Every schema file, with the topic it belongs at."""
    found = []
    for path in sorted(pathlib.Path(SCHEMA_DIR).rglob("*")):
        if path.is_file():
            under = path.relative_to(SCHEMA_DIR).with_suffix("")
            found.append(("schemas/" + under.as_posix(), path.read_text()))
    return found


def start_broker(workdir):
    """The broker this tour runs against, from examples/saguin.yaml.

    The file is used as written except for the address, which becomes a
    free port so this cannot collide with a broker you already have
    running.
    """
    with open(CONFIG) as f:
        written = f.read()
    running = Broker(
        broker_binary(), workdir,
        written.replace("127.0.0.1:1883", "127.0.0.1:{port}"),
    ).start()
    return running


# --- the tour ------------------------------------------------------------


def connecting(port):
    say("1. Connecting")
    note("A saguin client IS a paho client - paho's own methods are all there")
    note("and behave as they always have. `start` is the one addition: it")
    note("connects, runs the network loop and waits for the CONNACK, because")
    note("everything a client needs to know is in that answer.")
    code("""
client = saguin.Client("gateway-1", schema_registry="schemas")
client.start("{}", {})
""".format(HOST, port))
    client = saguin.Client("gateway-1", schema_registry="schemas")
    client.start(HOST, port)
    shown("connected as", client.name)
    shown("session kept for", "{}s (this client is not durable)".format(
        client.granted_session_expiry))
    note()
    note("**`schema_registry=` names the channel that holds your schemas.**")
    note("Here that is `schemas`, an ordinary `latest` channel in this")
    note("broker's configuration - a schema is a value in it like any other.")
    note("A client needs it only to serialize or read a payload through a")
    note("schema; without it everything else works and the schema verbs say")
    note("what is missing.")
    note()
    note("**It is named rather than worked out**, and that is a rule rather")
    note("than a convenience: a record points at its schema by topic, and an")
    note("ACL governs who may *write* a topic and never who may *name* one.")
    note("Without a registry to check against, a publisher could point you at")
    note("a `latest` channel holding device state and have you read it out.")
    note()
    note("A client that wants to keep its place asks for it, and the broker")
    note("says how long it will hold the position - which is not always what")
    note("was asked for, since it caps at max_session_expiry.")
    code("""
reader = saguin.Client("dashboard", durable=True, session_expiry=86400,
                       schema_registry="schemas")
""")
    reader = saguin.Client("dashboard", durable=True, session_expiry=86400,
                           schema_registry="schemas")
    reader.start(HOST, port)
    shown("asked for", "{}s".format(reader.session_expiry))
    shown("granted", "{}s  <- the broker's cap".format(
        reader.granted_session_expiry))
    shown("session found", "{} - the broker has no session for 'dashboard' "
          "yet".format(reader.session_present))
    note()
    note("`session_present` is the broker saying whether it still had this")
    note("client's session. False here because 'dashboard' is new. On a")
    note("later connection it is True, and the position comes back with it -")
    note("and False again would mean the session had gone, so the client")
    note("starts wherever a reader with no position starts.")
    return client, reader


def naming_a_channel(client):
    say("2. Naming a channel instead of a topic")
    note("A channel is a name and a topic filter, and the filter lives in the")
    note("operator's configuration - it is the one thing a client cannot work")
    note("out for itself. So the library asks the broker, once, and builds the")
    note("topic from the answer and the key you give.")
    code("""
info = client.channel("readings")    # ask the broker once; the answer is cached

client.append.publish("readings",
                      key=["site42", "device", "temp/1"],
                      value=b"21.5")
""")
    info = client.channel("readings")
    shown("channel", info.name)
    shown("type", info.type)
    shown("filter", info.filter)
    shown("you may", ", ".join(info.verbs))
    note()
    note("The filter has three levels that take an argument, and the key fills")
    note("them in order:")
    note("   +                 any one level          -> site42")
    note("   {device,sensor}   one of those two       -> device")
    note("   #                 the rest, or nothing   -> temp/1")
    sent = client.append.publish("readings", key=["site42", "device", "temp/1"],
                                 value=b"21.5")
    sent.wait_for_publish(5)
    did("published b'21.5' to iot/site42/device/temp/1 - one record so far")
    shown("record id", sent.saguin_id)
    note()
    note("The id is the library's doing: a UUIDv7 the broker stores as the")
    note("record's Message ID, stable across redelivery, dead-lettering and")
    note("replay. It is what a consumer deduplicates on.")


def reading(client, reader, port):
    say("3. Reading, and keeping your place")
    note("A durable client resumes where it stopped. Records are acknowledged")
    note("when the loop asks for the next one, never when they arrive - the")
    note("stored position advances on the acknowledgement, so acknowledging on")
    note("arrival would let a client that died half way through a record")
    note("resume AFTER it.")
    code("""
for record in reader.append.consume("readings", key=["site42"]):
    handle(record)
""")
    note()
    note("First, two more records. These carry headers of their own, so that")
    note("there is something on them beside what the broker stamps:")
    code("""
client.append.publish("readings", key=["site42", "device", "temp/1"],
                      value=b"22.0",
                      headers=[("unit", "C"), ("firmware", "1.4.2")])
""")
    for thing, n in (("device", b"22.0"), ("sensor", b"22.5")):
        client.append.publish("readings", key=["site42", thing, "temp/1"],
                              value=n,
                              headers=[("unit", "C"), ("firmware", "1.4.2")],
                              ).wait_for_publish(5)
        did("published {} to iot/site42/{}/temp/1, with unit=C and "
            "firmware=1.4.2".format(n, thing))
    note()
    note("So the channel holds three records now: b'21.5' from a moment ago,")
    note("and these two. A reader arriving late is served all three.")

    records = reader.append.consume("readings", key=["site42"], timeout=4)
    for record in records:
        shown(record.topic, "{} at offset {}, {}".format(
            record.payload, record.offset,
            record.timestamp.strftime("%H:%M:%S")))
        for name, value in record.properties.UserProperty or []:
            note("      {:<18} {}".format(name, value))
    note()
    note("**The topic is on every record**, which matters here because the")
    note("key narrowed the read rather than naming one topic: `site42` filled")
    note("the `+`, and the two levels after it were left open - so these")
    note("could have come from any device or sensor under that site, and")
    note("`record.topic` is what says which.")
    note()
    note("**And every User Property, exactly as it arrived.** The three under")
    note("`saguin-` are the broker's own - the record's identity, its")
    note("position and when the broker received it - and they are what")
    note("`record.id`, `record.offset` and `record.timestamp` are read from.")
    note("The rest are the publisher's, and `record.headers` is those alone:")
    note("a client may not write under the reserved prefix, so nothing there")
    note("can be forged.")
    note()
    note("`saguin-channel` is absent here, and that is not an omission: the")
    note("broker adds it only where a reader's filter reaches more than one")
    note("channel, since a reader whose filter reaches exactly one already")
    note("knows and would pay 26 bytes a message to be told.")
    note()
    note("All three, including the one published before this reader existed:")
    note("a client with no position is served the channel from its retention")
    note("floor. That replay is what the channel type is for.")
    note()
    note("A key left out is every topic that level can hold - and a {a,b}")
    note("level becomes one subscription per alternative rather than a +,")
    note("because a + there would also reach topics the channel does not")
    note("claim.")

    note()
    note("**And the place is kept.** Close this reader, publish while it is")
    note("away, and reconnect under the same id: the broker still holds its")
    note("session, and what arrives is what it missed - never the channel")
    note("over again.")
    did("closing 'dashboard' - the connection ends, the session stays")
    reader.close()
    did("publishing b'23.0' while nobody is reading")
    client.append.publish("readings", key=["site42", "device", "temp/1"],
                          value=b"23.0").wait_for_publish(5)
    did("reconnecting under the same id, durable again")
    reader = saguin.Client("dashboard", durable=True, session_expiry=86400,
                           schema_registry="schemas")
    reader.start(HOST, port)
    shown("session found", "{} - the broker kept 'dashboard' this "
          "time".format(reader.session_present))
    got = list(reader.append.consume("readings", key=["site42"], timeout=3))
    for r in got:
        shown(r.topic, "{} at offset {}".format(r.payload, r.offset))
    shown("records delivered", "{} - only what it missed".format(len(got)))
    return reader


def splitting(client, port):
    say("3b. Splitting a channel between readers")
    note("Several readers can take a share of one channel each, with no")
    note("coordination and nothing to configure. A reader says which share")
    note("is its own when it subscribes, and the broker delivers a record")
    note("only to the member whose share it falls in.")
    code("""
# member 1 of 2
for record in reader.append.consume("readings", key=["split"],
                                    topic_hash=(2, 1)):
    handle(record)
""")
    note()
    note("A reader declaring nothing gets everything, which is every reader")
    note("that has never heard of this.")
    note()

    topics = []
    for n in range(6):
        thing = "t{}".format(n)
        client.append.publish("readings", key=["split", "device", thing],
                              value=thing.encode()).wait_for_publish(5)
        topics.append("iot/split/device/{}".format(thing))
    did("published six records under iot/split/device/, away from site42")
    note()
    note("**Which member is owed which is not something you have to observe.**")
    note("`saguin.partition` is the same calculation the broker makes, and")
    note("it reaches no broker at all:")
    code("""
saguin.partition("iot/split/device/t0", 2)   # the member owed it
""")
    for topic in topics:
        shown(topic, "member {}".format(saguin.partition(topic, 2)))

    note()
    did("reading with two members, 0 of 2 and 1 of 2")
    got = {}
    for index in (0, 1):
        member = saguin.Client("split-{}".format(index), durable=True)
        member.start(HOST, port)
        try:
            got[index] = [r.topic for r in member.append.consume(
                "readings", key=["split"], topic_hash=(2, index), timeout=3)]
        finally:
            member.close()
    for index in (0, 1):
        for topic in got[index]:
            shown(topic, "arrived at member {}".format(index))

    # **Read back from what the members actually received**, not written
    # once as prose. A split that loses a record and a split that delivers
    # one twice both leave every member looking healthy, and only the
    # arithmetic over all of them shows it.
    delivered = [topic for held in got.values() for topic in held]
    twice = sorted({one for one in delivered if delivered.count(one) > 1})
    missed = sorted(set(topics) - set(delivered))
    shown("records split", len(delivered))
    shown("delivered twice", ", ".join(twice) if twice else "none")
    shown("never delivered", ", ".join(missed) if missed else "none")
    shown("members that got some", sum(1 for held in got.values() if held))
    note()
    if twice or missed:
        note("**That is a defect, and it is the one this section is for.**")
        note("Every record belongs to exactly one member of the two.")
    else:
        note("Every record to exactly one member - the three lines above are")
        note("what say so, read back from what the members received.")
    note()
    note("**Nothing tells you about a share nobody claimed.** The broker")
    note("cannot tell 'there is no member 1' from 'member 1 has not started")
    note("yet', so it says nothing and every reader that is running looks")
    note("healthy. Covering every share is the application's job, and the")
    note("calculation above is how it checks.")
    note()
    note("A member's position moves past the records outside its share, so")
    note("widening a share tomorrow recovers none of what it skipped")
    note("yesterday. And a share is refused on a shared subscription and on")
    note("a queue, because both already divide a stream between members.")


def beginning_and_jumping(client, port):
    say("4. Where to begin, and where to jump")
    note("Two verbs, because they answer two questions.")
    code("""
reader.append.consume("readings", start=0)   # only if it has no position
reader.append.seek("readings", to=0)         # always
""")
    note("`start` is where to begin when this client has never read here. It")
    note("fires ONCE - a value written into your code would otherwise replay")
    note("the whole channel on every restart.")
    note("`seek` is a deliberate move: replay a day after a bug, or skip a")
    note("backlog. It fires every time.")
    note()
    note("Both take saguin's own values and nothing invented:")
    note("   0        the retention floor - everything still held")
    note("   -1       the next offset - only what arrives after")
    note("   17       any offset you name - this one is just an example")
    note('   "12h"    the last twelve hours')
    note('   "7d"     the last seven days, or an RFC 3339 moment')

    note()
    did("connecting a new client, 'late-joiner', which has never read here")
    fresh = saguin.Client("late-joiner", durable=True, schema_registry="schemas")
    fresh.start(HOST, port)
    did("reading with start=-1, and publishing nothing after it")
    got = [r.payload for r in fresh.append.consume(
        "readings", key=["site42"], start=-1, timeout=2)]
    shown("start=-1", "{} records - everything above is behind it".format(len(got)))

    did("seeking the same client to 0, the retention floor")
    landed = fresh.append.seek("readings", 0)
    shown("seek to 0", "landed at offset {}".format(landed))
    did("reading again on that same client, now that it has been moved")
    got = [r.payload for r in fresh.append.consume(
        "readings", key=["site42"], timeout=3)]
    shown("then read", "{} records - the whole channel".format(len(got)))
    fresh.close()


def key_value(client, reader):
    say("5. State: the current value per key")
    note("A `latest` channel keeps the current value of every topic, and")
    note("whoever subscribes next is sent it - so a device that was away")
    note("learns the state without anybody replaying a log at it.")
    code("""
client.latest.set("state", key=["site42", "temp"], value=b"18")
client.latest.get("state", key=["site42", "temp"])      # b"18"
client.latest.delete("state", key=["site42", "temp"])
""")
    did("setting state/site42/temp to b'18'")
    client.latest.set("state", key=["site42", "temp"], value=b"18").wait_for_publish(5)
    shown("get", client.latest.get("state", key=["site42", "temp"]))
    did("setting the same key to b'19'")
    client.latest.set("state", key=["site42", "temp"], value=b"19").wait_for_publish(5)
    shown("after an update", client.latest.get("state", key=["site42", "temp"]))
    did("deleting it")
    client.latest.delete("state", key=["site42", "temp"]).wait_for_publish(5)
    shown("after a delete", client.latest.get("state", key=["site42", "temp"]))
    note()
    note("None for a deleted key, and None for one never set - the two have")
    note("always been the same answer here, so absence needs no new")
    note("vocabulary. `delete` is a write with nothing in it; it has a name")
    note("because 'publish an empty payload to delete it' is exactly the lore")
    note("this library exists to remove.")
    note()
    note("Following the state instead of asking for it:")
    code("""
for record in reader.latest.consume("state", key=["site42"]):
    record.is_catch_up   # True for the state you arrived to
""")
    did("setting state/site42/door to b'shut' before anybody subscribes")
    client.latest.set("state", key=["site42", "door"], value=b"shut").wait_for_publish(5)
    did("now subscribing - the value set a moment ago is what arrives first")
    following = reader.latest.consume("state", key=["site42"], timeout=3)
    first = next(following)
    shown("on subscribe", "{} is_catch_up={}".format(first.payload, first.is_catch_up))
    did("changing it to b'open' while that reader is subscribed")
    client.latest.set("state", key=["site42", "door"], value=b"open").wait_for_publish(5)
    change = next(following)
    shown("then a change", "{} is_catch_up={}".format(change.payload,
                                                      change.is_catch_up))
    following.close()
    note()
    note("**Let go of a reader when you have finished with it.** It holds")
    note("its subscription until its loop ends or it is closed - a `for`")
    note("that runs out closes itself, a bare `next` does not, and on a")
    note("queue that means going on being handed work nobody is reading.")


def work(client, port):
    say("6. Work: a queue, and a worker that fails")
    note("A queue hands each job to one worker at a time. Return normally and")
    note("the job is acked; raise and it is handed back and the worker carries")
    note("on to the next - the shape RabbitMQ's clients have, because a worker")
    note("that stopped on one bad job would stop everything behind it.")
    code("""
worker.queue.work("tasks", pack_the_order)
""")
    did("putting three jobs on the queue: job-1, job-2 and job-3")
    note("job-2's handler raises every time; the other two return normally.")
    for n in ("1", "2", "3"):
        info = client.queue.publish("tasks", key=["site42", n],
                                    value="job-{}".format(n).encode())
        info.wait_for_publish(5)
        shown("job-{} record id".format(n), info.saguin_id)

    attempts = []
    began = time.monotonic()

    def handler(job):
        attempts.append((job.payload, job.attempt))
        shown("handling", "{} attempt {}  +{:.1f}s".format(
            job.payload, job.attempt, time.monotonic() - began))
        if job.payload == b"job-2":
            raise RuntimeError("this one always fails")

    worker = saguin.Client("packer", durable=True, schema_registry="schemas")
    worker.start(HOST, port)
    worker.queue.work("tasks", handler, on_error=lambda job, e: None, timeout=8)
    note()
    # **Read back rather than asserted.** What the attempts were is
    # something this run knows; a sentence saying "job-2 came back twice"
    # is a sentence that goes on printing after it stops being true.
    tries = [a for p, a in attempts if p == b"job-2"]
    shown("job-2 attempts", "{} - {}".format(
        len(tries), ", ".join(str(a) for a in tries) or "none"))
    # Every handing rather than the distinct names, so that one of these
    # coming back twice shows up as its name twice.
    others = sorted(p.decode() for p, _ in attempts if p != b"job-2")
    shown("the other two", ", ".join(others) or "none")
    note()
    note("job-2 is handed back each time and comes straight back to this same")
    note("worker, since it is the one asking - with a gap that grows: this")
    note("queue is linear at 1s, so 1s, then 2s. The +Xs above is when each")
    note("attempt actually arrived, so the gaps are there to read rather than")
    note("taken on trust. After max_attempts it is dead-lettered, which is the")
    note("broker's answer to work that cannot be done.")

    say("6b. Two workers, which is what a queue is for")
    note("Each job goes to exactly one of them. Nothing is configured to make")
    note("that happen - it is what a queue channel does, and it is the whole")
    note("difference from an append channel, where every reader sees")
    note("everything.")
    code("""
# in two processes, or two threads, or two machines
worker.queue.work("tasks", handler)
""")
    did("putting six more jobs on: batch-0 to batch-5")
    did("starting two workers, packer-a and packer-b, in threads")
    for n in range(6):
        client.queue.publish("tasks", key=["site42", "batch-{}".format(n)],
                             value="batch-{}".format(n).encode()).wait_for_publish(5)

    # **Every handing, not the last one per job.** A dictionary keyed by
    # the job would overwrite the first worker's name with the second's,
    # so a job handed to both would print as six jobs handed out once
    # each - which is exactly the claim below. The worst thing a queue
    # can do is hand one job to two workers, and it must not be the one
    # thing this cannot show.
    handings = []
    guard = threading.Lock()

    def share(name):
        def handler(job):
            with guard:
                handings.append((job.payload.decode(), name))

        hand = saguin.Client(name, durable=True, schema_registry="schemas")
        hand.start(HOST, port)
        try:
            hand.queue.work("tasks", handler, timeout=5)
        finally:
            hand.close()

    hands = [threading.Thread(target=share, args=("packer-{}".format(n),))
             for n in ("a", "b")]
    for one in hands:
        one.start()
    for one in hands:
        one.join(timeout=60)

    for job, who in sorted(handings):
        shown(job, "handled by {}".format(who))
    jobs = [job for job, _ in handings]
    twice = sorted({job for job in jobs if jobs.count(job) > 1})
    busy = {who for _, who in handings}
    shown("jobs handed out", len(set(jobs)))
    shown("times handed out", len(handings))
    shown("workers that did some", len(busy))
    # **What this run did, rather than what a queue is supposed to do.**
    # A sentence written once goes on printing after it stops being true,
    # and this is the sentence a developer would believe.
    shown("handled twice", ", ".join(twice) if twice else "none")
    note()
    if twice:
        note("**That is a defect, and it is the one this section is for.** A")
        note("queue hands each job to one worker; the line above says one of")
        note("them went to two.")
    else:
        note("Every job handled exactly once - the line above is what says so,")
        note("and it is read back from what the handlers actually saw.")
    if len(busy) < 2:
        note("Only one worker got any of it this time. Nothing shares the work")
        note("out evenly: whichever worker asks is handed the next job, so a")
        note("worker that connects first can take the lot.")
    note("A job a worker takes is invisible to the other until its lease runs")
    note("out - which is why a queue needs no coordination between them.")
    return worker


def dead_letters(client, worker):
    say("7. Dead letters, and putting work back")
    note("A dead-letter channel is an `append` channel - its name is the")
    note("queue's with __dlq on the end - so it is read like any other. What")
    note("is different is on the record.")
    code("""
for record in reader.append.consume("tasks__dlq"):
    record.dlq.reason      # attempts_exhausted, or expired
    worker.queue.redrive("tasks", record)
""")
    did("reading tasks__dlq - job-2, whose handler kept raising, is there")
    dead = one(worker.append.consume("tasks__dlq", key=["site42"], timeout=15))
    shown("topic", dead.topic)
    shown("came from", dead.dlq.channel)
    shown("why", dead.dlq.reason)
    shown("attempts", dead.dlq.attempts)
    shown("dead-lettered at", dead.dlq.at.strftime("%H:%M:%S"))
    shown("its own id", dead.id)
    note()
    note("A publisher cannot forge any of that: everything sent under the")
    note("reserved saguin- prefix is stripped, so this is the broker's own")
    note("account.")
    note()
    note("Putting it back names THE QUEUE - the dead-letter channel is the")
    note("queue's own, so there is nothing to name twice. The __dlq level")
    note("comes off where the channel's filter puts it, which is not always")
    note("the end. This queue's filter is work/+/jobs/+, so the level is")
    note("last - but a queue filtered `bulk/#` would have its dead letters")
    note("at `bulk/__dlq/…`, second level, and stripping the last one would")
    note("put the work back on the wrong topic.")
    did("redriving it: queue.redrive(\"tasks\", record)")
    worker.queue.redrive("tasks", dead).wait_for_publish(5)
    did("fetching from the queue again to see it arrive")
    # A job is acknowledged before the reader is let go - leaving a loop
    # still holding one is the trap section 5 warns about.
    jobs = worker.queue.fetch("tasks", timeout=10)
    back = next(jobs)
    shown("back on the queue", "{} at {}".format(back.payload, back.topic))
    shown("attempt", back.attempt)
    shown("same record id", back.id == dead.id)
    worker.queue.ack(back)
    jobs.close()
    note()
    note("The dead letter itself stays where it is - reading an append")
    note("channel removes nothing - so redriving twice queues the work twice.")
    note("The id is what makes that something a consumer can notice.")


def schemas_tour(client, reader):
    say("8. Schemas")
    note("A schema registry needs nothing from the broker: a `latest` channel")
    note("is already a key-value store with delete, so a registry is a channel")
    note("and a convention. Registering is an ordinary set.")
    note()
    note("The schemas are files under examples/schemas/, and the tree is the")
    note("registry's layout: a file at acme/weather/v1.avsc is registered at")
    note("the topic schemas/acme/weather/v1. Two of them here, one Avro and")
    note("one protobuf.")
    code("""
for topic, text in schemas_on_disk():           # walks examples/schemas/
    client.latest.set("schemas", key=[topic.split("/", 1)[1]],
                      value=text.encode())
""")
    for topic, text in schemas_on_disk():
        client.latest.set("schemas", key=[topic.split("/", 1)[1]],
                          value=text.encode()).wait_for_publish(5)
        did("registered {} ({} bytes) from examples/schemas/".format(
            topic, len(text)))
    note()
    note("Now a record written against one of them. The value is a dict")
    note("rather than bytes, and `schema=` names the topic it was just")
    note("registered at:")
    code("""
client.append.publish("readings", key=["site42", "sensor", "t"],
                      value={"site": "site42", "temp": 21.5},
                      schema="schemas/acme/weather/v1")
""")

    following = reader.append.consume("readings", key=["site42", "sensor"],
                                      timeout=5)
    did("publishing a dict - not bytes - with schema= naming the Avro one")
    client.append.publish("readings", key=["site42", "sensor", "t"],
                          value={"site": "site42", "temp": 21.5},
                          schema="schemas/acme/weather/v1").wait_for_publish(5)
    record = one(following)
    shown("on the wire", "{!r}".format(record.payload))
    shown("as JSON it would be", "{} bytes, this is {}".format(
        len(json.dumps({"site": "site42", "temp": 21.5})), len(record.payload)))
    shown("Content Type", record.properties.ContentType)
    shown("schema", record.schema)
    shown("deserialized", record.deserialized)
    note()
    note("The pointer is a whole topic rather than an id, which is what makes")
    note("the convention work without an allocator: a bare name lets two")
    note("publishers in different domains choose the same one, the second")
    note("silently replacing the first.")
    note()
    note("There are no versions - the topic IS the identity, so a new version")
    note("is a new topic. And the schema must be registered before you")
    note("produce: a record written against a schema no consumer can fetch is")
    note("a record nobody can read.")

    note()
    note("**And protobuf, on the same client and the same call.** The library")
    note("reads which it is off the schema, so nothing here says `avro` or")
    note("`proto` - a `.proto` can only be read by protobuf and an Avro")
    note("schema is JSON, and the Content Type the publisher sends says which")
    note("it chose.")
    jobs = reader.queue.fetch("tasks", timeout=4)
    did("putting a job on the queue: {'order': 'A-1174', 'items': 3, "
        "'destination': 'site42'} against the protobuf schema")
    client.queue.publish(
        "tasks", key=["site42", "parcel"],
        value={"order": "A-1174", "items": 3, "destination": "site42"},
        schema="schemas/acme/parcel/v1",
    ).wait_for_publish(5)
    parcel = next(jobs)
    shown("on the wire", "{!r}".format(parcel.payload))
    shown("Content Type", parcel.properties.ContentType)
    shown("deserialized", parcel.deserialized)
    reader.queue.ack(parcel)
    jobs.close()

    note()
    note("**One schema, every way of writing.** `schema=` is on all four,")
    note("because it is on the publish underneath all of them - so a channel")
    note("type is never the reason a payload cannot be serialized.")
    code("""
client.latest.set("state", key=["site42", "reading"],
                  value={"site": "site42", "temp": 23.5},
                  schema="schemas/acme/weather/v1")
client.queue.publish("tasks", key=["site42", "serialized"],
                     value={"site": "site42", "temp": 23.5},
                     schema="schemas/acme/weather/v1")
""")
    value = {"site": "site42", "temp": 23.5}

    did("setting state/site42/reading to {} against the Avro schema".format(value))
    client.latest.set("state", key=["site42", "reading"], value=value,
                      schema="schemas/acme/weather/v1").wait_for_publish(5)
    got = reader.latest.get("state", key=["site42", "reading"])
    shown("latest, on the wire", "{} bytes".format(len(got)))

    following = reader.latest.consume("state", key=["site42", "reading"],
                                      timeout=4)
    shown("latest, deserialized", one(following).deserialized)

    jobs = reader.queue.fetch("tasks", timeout=4)
    did("putting the same value on the queue, same schema, same call shape")
    client.queue.publish("tasks", key=["site42", "serialized"], value=value,
                         schema="schemas/acme/weather/v1").wait_for_publish(5)
    job = next(jobs)
    shown("queue, on the wire", "{} bytes".format(len(job.payload)))
    shown("queue, deserialized", job.deserialized)
    reader.queue.ack(job)
    jobs.close()
    note()
    note("Broadcast is the fourth, and it comes next - it needs one extra")
    note("step on the way back, because reading a broadcast topic goes")
    note("through paho's own callback and hands you a paho message rather")
    note("than one of these.")


def broadcast(client, port):
    say("9. Broadcast: still ordinary MQTT")
    note("A topic no channel claims is plain MQTT, published the ordinary way")
    note("with paho's own verb. Nothing is stored and nothing is replayed.")
    code("""
def on_message(client, userdata, msg):
    record = client.record(msg)      # the one path that hands you a paho message
    client.ack(msg.mid, msg.qos)     # and the one you acknowledge yourself

listener.on_message = on_message     # paho's own callback
listener.subscribe("shout/#", qos=1) # paho's own subscribe

client.publish("shout/hello", {"site": "site42", "temp": 21.5},
               schema="schemas/acme/weather/v1")
""")
    import queue as q

    arrived = q.Queue()
    listener = saguin.Client("listener", schema_registry="schemas")
    listener.start(HOST, port)
    # **Acknowledged here, and that is the point of showing it.** This
    # client is in manual acknowledgement, which is what keeps a channel
    # record's acknowledgement from going out before the record has been
    # read - so a handler written for broadcast answers for its own.
    def took(cl, u, msg):
        arrived.put(msg)
        cl.ack(msg.mid, msg.qos)

    listener.on_message = took
    listener.subscribe("shout/#", qos=1)
    time.sleep(0.4)

    did("subscribing to shout/# with paho's own subscribe, then publishing")
    client.publish("shout/hello", {"site": "site42", "temp": 21.5},
                   schema="schemas/acme/weather/v1").wait_for_publish(5)
    record = listener.record(arrived.get(timeout=5))
    shown("topic", record.topic)
    shown("deserialized", record.deserialized)
    shown("record.offset", "{} - a broadcast has no position, because "
          "nothing keeps one".format(record.offset))
    listener.close()
    note()
    note("`schema=` works here too, because it is on paho's own publish. So")
    note("does everything else about a record - what it does not have is a")
    note("position, because nothing is keeping one.")
    note()
    note("The handler acknowledged its own record. A `consume` loop does that")
    note("for what it hands you; a handler you write for broadcast is the")
    note("reader, so it is the one that answers - and what it never")
    note("acknowledges, the broker sends again on the next connection.")


def hanging_up(client, port):
    say("10. Hanging up a client")
    note("It ends the connection and leaves the session alone, so the device")
    note("reconnects and resumes at its stored position - the whole cost is")
    note("one reconnection. On its own it withdraws nothing.")
    code("""
client.admin.disconnect("device-7")     # "hung-up", or "no-such-client"
""")
    did("connecting a second client called device-7, to hang up")
    victim = saguin.Client("device-7", schema_registry="schemas")
    victim.start(HOST, port)
    shown("device-7 connected", victim.is_connected())
    did("asking the broker to hang up 'device-7'")
    shown("device-7", client.admin.disconnect("device-7"))
    victim.loop_stop()
    did("asking it to hang up 'device-99', which nothing is using")
    shown("device-99", client.admin.disconnect("device-99"))
    note()
    note("The two answers are worth keeping apart, which is the whole reason")
    note("there is a reply: otherwise 'that device went away an hour ago' and")
    note("'you have misspelled the id' read the same, and the second is the")
    note("one an operator actually sends.")
    note()
    note("Note it is client.admin.disconnect and not client.disconnect -")
    note("paho's own disconnect hangs up YOU.")


def what_goes_wrong(client, reader, port):
    say("11. What goes wrong, and what it tells you")
    note("Every refusal below is one you can meet. They are here because an")
    note("error that does not say what to do next is a defect in this library,")
    note("and these are the ones a developer meets first.")

    note()
    note("A key that does not fit the channel's filter - caught before")
    note("anything is sent, and it names the filter:")
    try:
        client.append.publish("readings", key=["site42", "gadget", "t"], value=b"1")
    except saguin.KeyDoesNotFit as e:
        broke(e)
    try:
        client.append.publish("readings", key=["site42/west", "device", "t"], value=b"1")
    except saguin.KeyDoesNotFit as e:
        broke(e)
    try:
        client.append.publish("readings", key=["site42"], value=b"1")
    except saguin.KeyDoesNotFit as e:
        broke(e)

    note()
    note("A verb for the wrong kind of channel. The three writes are one")
    note("publish underneath and the reads are one subscribe, so this is the")
    note("only thing between appending to a log and queueing work by mistake.")
    note("**Every verb that names a channel checks**, not the ones somebody")
    note("remembered - here is one from each group, and the message says what")
    note("the channel is and which verbs it takes:")
    for call in (
        lambda: client.append.publish("tasks", key=["site42", "1"], value=b"1"),
        lambda: client.append.seek("state", 0),
        lambda: client.latest.set("readings", key=["site42", "device", "t"], value=b"1"),
        lambda: client.latest.get("tasks", key=["site42", "1"]),
        lambda: client.queue.publish("readings", key=["site42", "device", "t"],
                                     value=b"1"),
        lambda: client.queue.redrive("state", None),
    ):
        try:
            call()
        except saguin.WrongChannelType as e:
            broke(e)
    note()
    note("`ack` and `nack` are the two that name no channel: a job carries")
    note("where to answer, so there is nothing to be wrong about.")

    note()
    note("A channel the broker will not tell you about. Note what it does NOT")
    note("say: whether the channel exists. A channel you hold no verb on and a")
    note("channel that does not exist are the same answer, so asking cannot be")
    note("used to discover what a broker has.")
    try:
        client.channel("no-such-channel")
    except saguin.UnknownChannel as e:
        broke(e)

    note()
    note("A schema that is not registered - caught before the record leaves,")
    note("which is the whole reason a schema must be registered first:")
    try:
        client.append.publish("readings", key=["site42", "sensor", "t"],
                              value={"site": "site42", "temp": 1.0},
                              schema="schemas/never/registered")
    except schemas.SchemaError as e:
        broke(e)

    note()
    note("A payload that does not fit its schema:")
    try:
        client.append.publish("readings", key=["site42", "sensor", "t"],
                              value={"site": "site42"},
                              schema="schemas/acme/weather/v1")
    except schemas.SchemaError as e:
        broke(e)

    note()
    note("A schema pointer outside the registry. An ACL governs who may WRITE")
    note("a topic and never who may NAME one, so a publisher could otherwise")
    note("point you at a latest channel holding device state and have you read")
    note("it out:")
    try:
        reader.schema_text("state/site42/temp")
    except schemas.SchemaError as e:
        broke(e)

    note()
    note("A seek the broker will not make - its own words, carried back:")
    try:
        reader.append.seek("readings", "not-a-position")
    except saguin.RequestRefused as e:
        broke(e)

    note()
    note("And a question asked on a client that has closed - every verb that")
    note("asks the broker needs a connection for the answer to arrive on.")
    note("Deserializing is the one that bites in practice: `record.deserialized`")
    note("fetches a schema, so deserialize inside the reading loop, not after")
    note("the client has gone:")
    gone = saguin.Client("gone-away", schema_registry="schemas")
    gone.start(HOST, port)
    gone.close()
    try:
        gone.channel("readings")
    except RuntimeError as e:
        broke(e)


def main():
    workdir = tempfile.mkdtemp(prefix="saguin-demo-")
    try:
        running = start_broker(workdir)
    except RuntimeError as why:
        raise SystemExit(str(why))
    port, config = running.port, running.config
    clients = []
    # **Ctrl-C stops the broker rather than orphaning it.** The tour
    # starts a process of its own, so quitting between sections - which is
    # a reasonable thing to do - must not leave one running on a port
    # nobody remembers.
    try:
        say("0. The broker this tour runs against")
        note("examples/saguin.yaml, started for you on a free port - an")
        note("ordinary saguin configuration you can read, edit and run")
        note("yourself with `saguin -config examples/saguin.yaml`. Four")
        note("channels; anything else is broadcast.")
        shown("config", config)
        shown("listening on", "{}:{}".format(HOST, port))
        note()
        note("Printed without its comments, which are most of what the file")
        note("is: every channel in it says what that channel type does and")
        note("why each setting is what it is. Worth opening.")
        note()
        for line in open(config):
            line = line.rstrip()
            if line and not line.lstrip().startswith("#"):
                print("   {}{}{}".format(DIM, line, OFF))
        wait()

        client, reader = connecting(port)
        clients += [client, reader]
        wait()
        naming_a_channel(client)
        wait()
        reader = reading(client, reader, port)
        clients.append(reader)
        wait()
        splitting(client, port)
        wait()
        beginning_and_jumping(client, port)
        wait()
        key_value(client, reader)
        wait()
        worker = work(client, port)
        clients.append(worker)
        wait()
        dead_letters(client, worker)
        wait()
        schemas_tour(client, reader)
        wait()
        broadcast(client, port)
        wait()
        hanging_up(client, port)
        wait()
        what_goes_wrong(client, reader, port)

        say("Done")
        note("Everything above is in the README, and every claim in it is a")
        note("test in tests/ that runs against a broker like this one.")
    except KeyboardInterrupt:
        print("\n\n{}== Stopped{}".format(BOLD, OFF))
        note("Shutting the broker down and clearing up after it.")
    finally:
        for one in clients:
            try:
                one.close()
            except Exception:  # noqa: BLE001 - tearing down, not testing
                pass
        running.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
