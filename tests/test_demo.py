"""The guided tour is run, not just shipped.

A demo nobody runs rots into a document about a library that has moved on,
and it is the first thing a developer reads. So the suite runs the whole
of it against a real broker and checks it reached the end - and that each
section actually happened, rather than the tour quietly skipping one.
"""

import os
import pathlib
import re
import subprocess
import sys
import time

DEMO = pathlib.Path(__file__).resolve().parent.parent / "examples" / "demo.py"


def test_the_guided_tour_runs_to_the_end(broker):
    done = subprocess.run(
        [sys.executable, str(DEMO)],
        env=dict(os.environ, SAGUIN_DEMO_PAUSE="0"),
        capture_output=True, text=True, timeout=300,
    )
    plain = re.sub(r"\x1b\[[0-9;]*m", "", done.stdout)
    assert done.returncode == 0, (
        "the tour exited {}:\n{}\n{}".format(done.returncode, plain[-3000:],
                                             done.stderr[-2000:])
    )

    # **Counted, not spot-checked.** A tour that stopped half way through
    # still prints a lot, and a check for one line would pass over the
    # rest.
    sections = re.findall(r"^== (\d+)\.", plain, re.M)
    assert sections == [str(n) for n in range(12)], (
        "the tour ran sections {} - it should run 0 to 11 in order".format(sections)
    )
    assert "== Done" in plain

    # The things a reader is there for, each from a different section.
    for wanted in (
        "iot/+/{device,sensor}/#",       # the filter it composes against
        "attempts_exhausted",            # a dead letter saying why
        "back on the queue",             # and being put back
        "application/avro",              # a schema chosen and named
        "application/x-protobuf",        # and the other format, from a file
        "latest, deserialized",          # and used on every channel type
        "queue, deserialized",
        "workers that did some",         # two workers sharing a queue
        "hung-up",                       # an operator's verb
        "no-such-client",                # and the answer that is not it
    ):
        assert wanted in plain, "the tour never showed {!r}".format(wanted)

    # **The queue's own promise, read out of the tour's numbers rather
    # than out of its prose.** The label was all this checked, so a tour
    # that printed one worker, or a job handed to two of them, passed.
    # Handing one job to two workers is the worst thing a queue can do,
    # and the tour is where a developer would meet it.
    def figure(label):
        found = re.findall(r"^   {}\s+(.+?)\s*$".format(re.escape(label)),
                           plain, re.M)
        assert len(found) == 1, (
            "expected one {!r} line, read {}: {}".format(label, len(found), found)
        )
        return found[0]

    # **The split, read out of the tour's numbers rather than its prose.**
    # A record delivered to two members and a record delivered to none
    # both leave every member looking healthy, so the tour has to account
    # for all six. The topics are fixed strings and the hash is a constant
    # of the protocol, so the 3/3 split below is not a coin toss.
    assert figure("records split") == "6"
    assert figure("delivered twice") == "none"
    assert figure("never delivered") == "none"
    assert figure("members that got some") == "2", (
        "one member took the whole channel, so the tour showed no split"
    )

    assert figure("jobs handed out") == "6"
    assert figure("times handed out") == "6", (
        "six jobs were handed out more than six times, so one of them went "
        "to two workers"
    )
    assert figure("handled twice") == "none"
    assert int(figure("workers that did some")) >= 1

    # The failing job's own account, from the section before it. The queue
    # is configured for three attempts with a growing gap, and the tour is
    # what shows a developer that is what happens.
    assert figure("job-2 attempts") == "3 - 1, 2, 3"
    assert figure("the other two") == "job-1, job-3", (
        "a job that succeeded was handed to this worker more than once"
    )


def test_the_tour_shows_every_error_a_developer_meets(broker):
    """Section 11 is the half of a tour that usually goes missing: what
    comes back when the call is wrong. Each of these is a class this
    library raises, and a tour that stopped showing one would leave a
    developer to meet it in production instead."""
    done = subprocess.run(
        [sys.executable, str(DEMO)],
        env=dict(os.environ, SAGUIN_DEMO_PAUSE="0"),
        capture_output=True, text=True, timeout=300,
    )
    plain = re.sub(r"\x1b\[[0-9;]*m", "", done.stdout)
    # Any exception name the tour printed, rather than a list of the
    # suffixes I happened to think of - the first version of this matched
    # `Error` and `Refused` and silently missed `WrongChannelType`.
    shown = set(re.findall(r"^   ([A-Z][A-Za-z]+): ", plain, re.M))
    assert len(shown) >= 6, "read only {} error names: {}".format(len(shown), shown)
    assert shown >= {
        "KeyDoesNotFit",
        "WrongChannelType",
        "UnknownChannel",
        "SchemaError",
        "RequestRefused",
        "RuntimeError",
    }, "the tour stopped showing some of the errors: {}".format(sorted(shown))


def test_ctrl_c_stops_the_broker_the_tour_started(broker):
    """The tour starts a broker of its own, so quitting part way through -
    a reasonable thing to do - must not leave one running on a port
    nobody remembers.

    **The signal has to be delivered deliberately.** A background job from
    a non-interactive shell inherits SIGINT *ignored*, so a child started
    the ordinary way never sees a Ctrl-C at all - which is how this was
    reported broken twice while it was working. The handler is put back to
    the default in the child before anything is sent.
    """
    import glob
    import signal

    before = set(glob.glob("/tmp/saguin-demo-*"))
    running = subprocess.Popen(
        [sys.executable, "-u", str(DEMO)],
        env=dict(os.environ, SAGUIN_DEMO_PAUSE="0"),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
    )
    try:
        time.sleep(6)          # let it get well into the tour
        assert running.poll() is None, "the tour finished before it was interrupted"
        running.send_signal(signal.SIGINT)
        out, _ = running.communicate(timeout=60)
    finally:
        if running.poll() is None:
            running.kill()
            running.communicate()

    plain = re.sub(r"\x1b\[[0-9;]*m", "", out)
    assert running.returncode == 0, (
        "interrupting the tour left it exiting {}:\n{}".format(
            running.returncode, plain[-2000:])
    )
    assert "== Stopped" in plain, plain[-2000:]
    assert set(glob.glob("/tmp/saguin-demo-*")) == before, (
        "the tour left its working directory behind, so it left a broker too"
    )


def test_the_tour_runs_with_nobody_to_press_a_key(broker):
    """Piped or redirected there is nobody to press [ENTER], and waiting
    for one is a crash rather than a pause. It used to be: the first
    section ended in EOFError."""
    done = subprocess.run(
        [sys.executable, "-u", str(DEMO)],
        stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=300,
        env={k: v for k, v in os.environ.items() if k != "SAGUIN_DEMO_PAUSE"},
    )
    plain = re.sub(r"\x1b\[[0-9;]*m", "", done.stdout)
    assert done.returncode == 0, plain[-2000:] + done.stderr[-1000:]
    assert "== Done" in plain
