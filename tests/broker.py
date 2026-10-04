"""Starting a broker, and finding the binary to start it with.

**One place, because there were two.** The test harness and
`examples/demo.py` each had their own copy of "where is the broker, and
how do I wait for it to listen" - the same rule twice, with two different
refusals when it was not there. The demo imports this rather than keeping
a second copy, which is the right way round: starting a broker is test
machinery, and the tour is a consumer of it.
"""

import os
import shutil
import socket
import subprocess
import time

# Names the broker to run against. Until saguin publishes a release
# binary there is nothing to install, so this points at one you built:
# `make build` in a saguin checkout writes ./bin/saguin.
BROKER_ENV = "SAGUIN_BROKER"


def broker_binary():
    named = os.environ.get(BROKER_ENV)
    if named:
        if not os.path.isfile(named) or not os.access(named, os.X_OK):
            raise RuntimeError(
                "{}={} is not an executable file".format(BROKER_ENV, named)
            )
        return named
    found = shutil.which("saguin")
    if found:
        return found
    raise RuntimeError(
        "no saguin broker to test against: set {} to the binary, or put "
        "`saguin` on PATH. `make build` in a saguin checkout writes "
        "./bin/saguin.".format(BROKER_ENV)
    )


def a_free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Broker:
    """A saguin process, its configuration, and everything it said.

    The log is kept and printed on any failure to start or stop, because
    a check that reports "the broker did not come up" without the
    broker's own words sends the reader to guess.
    """

    def __init__(self, binary, workdir, config):
        """`config` is the configuration to run, as text.

        **Passed in rather than known here**, because the two callers want
        different brokers: the suite wants channels to test against, and
        the tour wants `examples/saguin.yaml` - the file a reader can
        start themselves. What they share is everything below.
        """
        self.binary = binary
        self.config_text = config
        self.workdir = workdir
        self.port = a_free_port()
        self.ws_port = a_free_port()
        self.address = ("127.0.0.1", self.port)
        self.guarded = ("127.0.0.1", self.ws_port)
        """The door that wants a password: a WebSocket listener, since a
        broker takes one listener of each kind and the plain one is in
        use."""
        self.passwd = os.path.join(workdir, "clients.passwd")
        self.config = os.path.join(workdir, "saguin.yaml")
        self.logfile = os.path.join(workdir, "saguin.log")
        self.proc = None
        self._log = None

    def start(self):
        if "{passwd}" in self.config_text:
            subprocess.run(
                [self.binary, "--passwd", "add", self.passwd, "operator", "hunter2"],
                capture_output=True, text=True, check=True,
            )
        with open(self.config, "w") as f:
            # **Replaced rather than formatted.** A channel filter may
            # carry `{a,b}` - a level with a fixed set of spellings - and
            # `format` reads that as a field to substitute. It has bitten
            # twice; naming the three placeholders cannot.
            written = self.config_text
            for name, value in (("{port}", self.port), ("{ws_port}", self.ws_port),
                                ("{passwd}", self.passwd)):
                written = written.replace(name, str(value))
            f.write(written)

        # The broker's own reading of the file, before anything blames the
        # network for a typo in it.
        checked = subprocess.run(
            [self.binary, "--check-config", self.config],
            capture_output=True,
            text=True,
        )
        if checked.returncode != 0:
            raise RuntimeError(
                "the broker refused the test configuration:\n"
                + checked.stdout
                + checked.stderr
            )

        self._log = open(self.logfile, "w")
        self.proc = subprocess.Popen(
            [self.binary, "-config", self.config],
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        self._wait_for_the_port()
        return self

    def _wait_for_the_port(self, timeout=15.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    "the broker exited with {} before it listened:\n{}".format(
                        self.proc.returncode, self.said()
                    )
                )
            try:
                with socket.create_connection(self.address, 0.25):
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(
            "the broker did not listen on {} within {}s:\n{}".format(
                self.port, timeout, self.said()
            )
        )

    def said(self):
        if self._log is not None and not self._log.closed:
            self._log.flush()
        try:
            with open(self.logfile) as f:
                return f.read()
        except OSError:
            return "(no log)"

    def stop(self):
        if self.proc is None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
            raise RuntimeError(
                "the broker ignored SIGTERM and had to be killed:\n" + self.said()
            )
        finally:
            if self._log is not None:
                self._log.close()

    def check_it_stayed_up(self):
        """A crashed broker makes every later test fail for the wrong
        reason, and the run reports on a broker it was not driving."""
        if self.proc.poll() is not None:
            raise RuntimeError(
                "the broker exited during the run with {}:\n{}".format(
                    self.proc.returncode, self.said()
                )
            )
