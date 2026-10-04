"""A real broker, started for the suite, and a reader that is not the SDK.

Two rules shape this file.

**The suite drives a running broker rather than a mock.**  What is being
tested here is what saguin does with what a client sent, and a mock would
answer with what this SDK believes saguin does - which is the thing under
test.

**The oracle is what saguin promises, and the reader is plain paho.**
The promise each test measures against is written out at the test rather
than cited: saguin's specification lives in the broker's own repository,
and a reference a reader of this one cannot open is worse than no
reference.  Every
assertion about what the broker stamps on a delivery is checked against a
message read by ``paho.mqtt.client.Client``, not by anything in this
package.  A probe that asks the code under test what the answer should be
asserts only that the code agrees with itself.
"""

import os
import queue
import shutil
import subprocess
import sys
import tempfile
import uuid

import pytest
from broker import BROKER_ENV, Broker, a_free_port, broker_binary  # noqa: F401
from paho.mqtt import client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

# The broker is another repository and another language, so there is no
# building it from here.  Until saguin publishes a release binary this
# names the one you built: `make build` in a saguin checkout writes
# ./bin/saguin.
BROKER_ENV = "SAGUIN_BROKER"

CONFIG = """\
# Written by saguin-python's test suite.  Memory storage throughout: what
# these tests ask about is what the broker puts on a delivery, and none of
# them restarts it.
broker:
  id: saguin-python-tests
  log_level: warn
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:{port}
      # A second door, and it wants a password. The tcp one above admits
      # everybody, which is what every other test needs - and it left
      # `ConnectRefused` unreachable, so the one error path on the way in
      # had never been driven.
      ws:
        address: 127.0.0.1:{ws_port}
        password_file: {passwd}
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - mem:
          type: memory
          snapshot_dir: none
  retained:
    storage: mem
    retention_period: none
  limits:
    # Deliberately far below the broker's own default, so that a consumer
    # asking for more than the cap can be driven rather than described.
    max_session_expiry: 5m
channels:
  # A schema registry is a `latest` channel and a convention: the schema
  # text is the value, the topic is the identity, and a producer names
  # that topic in a `schema` user property.
  - schemas:
      type: latest
      filter: schemas/#
  # events and state share a prefix and the queue does not, so that
  # `iot/<site>/#` is a filter reaching exactly two channels - which is
  # the condition saguin stamps `saguin-channel` under.  An ordinary
  # subscription may not cross a queue's filter at all, so a spanning
  # filter that also reached the queue would be refused rather than
  # answering the question.
  - events:
      type: append
      filter: iot/+/events/+
  # A braced level, which is a level with a fixed set of spellings rather
  # than a wildcard - the shape the channel-level API composes against.
  - readings:
      type: append
      filter: iot/+/{device,sensor}/#
  - state:
      type: latest
      filter: iot/+/state/+
  # Named apart from the first level of its own filter on purpose: the
  # subscription pin is `$saguin/queue/<name>`, which names the queue and
  # not its topics, so a pin built from the filter instead of the name
  # would be wrong here and would go unnoticed if the two agreed.
  - tasks:
      type: queue
      filter: work/+/jobs/+
      # One attempt and a short lease, so that a returned job is
      # dead-lettered while a test is still watching rather than a minute
      # later.
      visibility_timeout: 2s
      retry:
        max_attempts: 1
  # `tasks` is configured for one attempt so that a handed-back job is
  # dead-lettered at once, which is what the dead-letter tests need. This
  # one retries, which is what a test about handing work back needs - the
  # two cannot be the same queue.
  - retried:
      type: queue
      filter: again/+/jobs/+
      visibility_timeout: 2s
      retry:
        max_attempts: 5
  # The same, with a gap that grows between attempts. `linear` is
  # base x attempt, so 1s, 2s, 3s - the shortest the broker will hold,
  # which it keeps in whole seconds and refuses anything under. Well clear
  # of the queue's own 200ms tick, which is what a gap has to be told
  # apart from. Only a job a worker *handed back* waits like this; one
  # taken back by the visibility timeout has already waited longer.
  # A queue whose filter ends in `#`, so its dead letters land at
  # `bulk/__dlq/...` rather than at the end - which is the case that makes
  # putting work back a matter of asking the filter rather than stripping
  # the last level.
  - bulk:
      type: queue
      filter: bulk/#
      visibility_timeout: 2s
      retry:
        max_attempts: 1
  - backoff:
      type: queue
      filter: slow/+/jobs/+
      visibility_timeout: 30s
      retry:
        max_attempts: 4
        backoff: linear
        backoff_base: 1s
"""


@pytest.fixture(scope="session")
def broker():
    workdir = tempfile.mkdtemp(prefix="saguin-python-tests-")
    running = Broker(broker_binary(), workdir, CONFIG).start()
    try:
        yield running
    finally:
        try:
            running.check_it_stayed_up()
        finally:
            running.stop()
            print(running.said(), file=sys.stderr)
            shutil.rmtree(workdir, ignore_errors=True)


@pytest.fixture
def site():
    """One topic level of this test's own, so that a session-long broker
    with memory storage never serves one test another's records."""
    return uuid.uuid4().hex[:12]


class Reader:
    """A plain paho MQTT 5 subscriber that collects what arrives.

    It reads and never publishes.  A helper that does both swallows the
    packets arriving behind the one it was showing you, and then reports
    an empty channel about a broker that delivered.
    """

    def __init__(self, address, client_id=None, clean_start=True, expiry=None):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id or "reader-" + uuid.uuid4().hex[:8],
            protocol=mqtt.MQTTv5,
        )
        self.arrived = queue.Queue()
        self.granted = queue.Queue()
        self.client.on_message = lambda c, u, m: self.arrived.put(m)
        self.client.on_subscribe = lambda c, u, mid, codes, props: self.granted.put(
            codes
        )
        self.address = address
        self._clean_start = clean_start
        self._expiry = expiry

    def __enter__(self):
        props = None
        if self._expiry is not None:
            props = Properties(PacketTypes.CONNECT)
            props.SessionExpiryInterval = self._expiry
        self.client.connect(
            self.address[0],
            self.address[1],
            clean_start=self._clean_start,
            properties=props,
        )
        self.client.loop_start()
        return self

    def __exit__(self, *exc):
        self.client.loop_stop()
        self.client.disconnect()

    def subscribe(self, topic, qos=1, options=None, properties=None):
        """Subscribe and wait for the SUBACK, answering its reason codes,
        so that a test never publishes into a subscription the broker has
        not yet made.

        ``properties`` is how a test puts a `saguin-filter` declaration on
        the packet itself, which is the only way to ask this broker about
        a spelling the SDK refuses before sending."""
        self.client.subscribe(topic, qos=qos, options=options,
                              properties=properties)
        return self.granted.get(timeout=10)

    def next(self, timeout=10):
        return self.arrived.get(timeout=timeout)

    def answer(self, job, outcome):
        """Answer a queue offer: `ack` or `return`.

        On this client rather than another, which is the protocol rather
        than a convenience - the broker takes an answer only from the
        session holding the job, so a second connection publishing the
        same words is ignored and the job comes back on its lease.
        """
        props = Properties(PacketTypes.PUBLISH)
        props.CorrelationData = job.properties.CorrelationData
        info = self.client.publish(
            job.properties.ResponseTopic, outcome, qos=1, properties=props
        )
        info.wait_for_publish(10)
        return info


@pytest.fixture
def reader(broker):
    def make(**kw):
        return Reader(broker.address, **kw)

    return make


class Producer:
    """An SDK client, connected, with the CONNACK already answered.

    Waiting for it matters: a test that publishes into a connection the
    broker has not yet accepted is a test whose timing decides whether it
    passed.
    """

    def __init__(self, address, client_id=None, **kw):
        import saguin

        kw.setdefault("schema_registry", "schemas")
        self.client = saguin.Client(
            client_id or "producer-" + uuid.uuid4().hex[:8], **kw
        )
        self.connected = queue.Queue()
        self.client.on_connect = (
            lambda c, u, flags, reason_code, props: self.connected.put(reason_code)
        )
        self.address = address
        self.reason_code = None

    def __enter__(self):
        self.client.connect(self.address[0], self.address[1], clean_start=True)
        self.client.loop_start()
        self.reason_code = self.connected.get(timeout=10)
        return self

    def __exit__(self, *exc):
        self.client.loop_stop()
        self.client.disconnect()

    def publish(self, *a, **kw):
        """Publish and wait for the PUBACK, so that a refusal is a failure
        here rather than a missing message somewhere later."""
        info = self.client.publish(*a, **kw)
        info.wait_for_publish(10)
        return info


@pytest.fixture
def producer(broker):
    with Producer(broker.address) as p:
        yield p


@pytest.fixture
def make_producer(broker):
    import contextlib

    with contextlib.ExitStack() as stack:

        def make(**kw):
            return stack.enter_context(Producer(broker.address, **kw))

        yield make


# -- a broker behind TLS ----------------------------------------------------

# **A second broker rather than a second listener on the first.** A broker
# takes one listener of each kind, and the session broker's two are
# already spoken for - the plain one every other test connects to, and the
# WebSocket one that wants a password. Putting TLS on either would change
# what those tests are about.
#
# Both listeners here carry the same certificate and differ in one thing:
# the WebSocket one names a `client_ca_file`, so it asks every client for
# a certificate and the plain-TLS one does not. That is the pair the two
# halves of this need, and it is also what "per listener rather than per
# broker" means in practice.
TLS_CONFIG = """\
broker:
  id: saguin-python-tls-tests
  log_level: warn
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:{port}
        tls:
          cert_file: CERTS/cert.pem
          key_file: CERTS/key.pem
      ws:
        address: 127.0.0.1:{ws_port}
        tls:
          cert_file: CERTS/cert.pem
          key_file: CERTS/key.pem
          client_ca_file: CERTS/ca.pem
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - mem:
          type: memory
          snapshot_dir: none
  retained:
    storage: mem
    retention_period: none
channels:
  - events:
      type: append
      filter: iot/+/events/+
"""


def make_certificates(into):
    """One authority, a certificate for the broker and one for a device.

    **The README's own recipe, run rather than quoted.** It is the three
    openssl commands per certificate that a reader of this project is told
    to type, so running them here is what says they still work - a
    recipe nobody runs rots the same way a demo nobody runs does.

    The broker's subjectAltName is the address the tests dial. A Common
    Name alone fails modern verification, which is the mistake this is
    most likely to be copied into.
    """
    if shutil.which("openssl") is None:
        raise RuntimeError(
            "no openssl to make test certificates with, so the TLS tests "
            "cannot run. openssl 3 or newer; -copy_extensions is what "
            "carries the subjectAltName into the signed certificate."
        )

    def run(*args):
        done = subprocess.run(args, capture_output=True, text=True)
        if done.returncode != 0:
            raise RuntimeError("{}\n{}{}".format(args, done.stdout, done.stderr))

    at = lambda name: os.path.join(into, name)  # noqa: E731
    run("openssl", "req", "-x509", "-newkey", "ec",
        "-pkeyopt", "ec_paramgen_curve:P-256", "-days", "2", "-nodes",
        "-subj", "/CN=saguin-python-tests-ca",
        "-keyout", at("ca-key.pem"), "-out", at("ca.pem"))
    run("openssl", "req", "-newkey", "ec",
        "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-subj", "/CN=broker",
        "-addext", "subjectAltName=IP:127.0.0.1",
        "-keyout", at("key.pem"), "-out", at("broker.csr"))
    run("openssl", "x509", "-req", "-in", at("broker.csr"),
        "-CA", at("ca.pem"), "-CAkey", at("ca-key.pem"),
        "-CAcreateserial", "-days", "2", "-copy_extensions", "copy",
        "-out", at("cert.pem"))
    # The Common Name is the client's name: it becomes the user name and
    # matches ACL patterns, and the password file is not consulted for it.
    run("openssl", "req", "-newkey", "ec",
        "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-subj", "/CN=device-7",
        "-keyout", at("device-7-key.pem"), "-out", at("device-7.csr"))
    run("openssl", "x509", "-req", "-in", at("device-7.csr"),
        "-CA", at("ca.pem"), "-CAkey", at("ca-key.pem"),
        "-CAcreateserial", "-days", "2", "-out", at("device-7.pem"))


@pytest.fixture(scope="session")
def tls_broker():
    """A broker behind TLS, and the certificates to meet it with.

    The files are in `tls_broker.workdir`: `ca.pem` is the authority that
    signed both sides, `device-7.pem` and `device-7-key.pem` are a
    client's.
    """
    workdir = tempfile.mkdtemp(prefix="saguin-python-tls-")
    make_certificates(workdir)
    running = Broker(broker_binary(), workdir,
                     TLS_CONFIG.replace("CERTS", workdir)).start()
    try:
        yield running
    finally:
        try:
            running.check_it_stayed_up()
        finally:
            running.stop()
            print(running.said(), file=sys.stderr)
            shutil.rmtree(workdir, ignore_errors=True)
