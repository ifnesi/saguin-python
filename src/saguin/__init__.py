"""saguin - a Python client library for interacting with the Saguin MQTT 5
broker, built as a thin layer over Eclipse Paho.

``saguin.Client`` is a paho client, so anything paho does it does. What it
adds sits beside that, grouped by the kind of channel it works on:
``client.append``, ``client.latest``, ``client.queue`` and
``client.admin``.
"""

from .channels import ChannelInfo, KeyDoesNotFit, partition, topic_hash
from .client import (
    DEFAULT_SESSION_EXPIRY,
    Client,
    ConnectRefused,
    PublishInfo,
    RequestRefused,
    SubscriptionRefused,
    UnknownChannel,
    WrongChannelType,
    new_message_id,
)
from .message import DeadLetter, Headers, Message, RESERVED_PREFIX

__all__ = [
    "ChannelInfo",
    "Client",
    "ConnectRefused",
    "DEFAULT_SESSION_EXPIRY",
    "DeadLetter",
    "Headers",
    "KeyDoesNotFit",
    "Message",
    "partition",
    "PublishInfo",
    "RequestRefused",
    "RESERVED_PREFIX",
    "SubscriptionRefused",
    "UnknownChannel",
    "topic_hash",
    "WrongChannelType",
    "new_message_id",
]

__version__ = "0.1.0"
