"""Relay-owned execution-channel configuration.

Embedded beside the bootstrap and written to ``/relay/_relay_config.py`` in the
runtime image.
"""

import os
from dataclasses import dataclass

from _relay_protocol import is_lower_hex_64

EXECUTION_PORT_ENV = "RELAY_EXECUTION_PORT"
EXECUTION_SECRET_ENV = "RELAY_EXECUTION_SECRET"


@dataclass(frozen=True)
class ExecutionConfig:
    """The port and HMAC key Relay assigned to this container's channel."""

    secret: str
    port: int


def load_execution_config() -> ExecutionConfig:
    """Read and validate Relay's channel configuration, then remove the secret
    from this process's environment before any handler module can be imported.

    Both values are Relay-owned and fail closed: a missing or malformed port or
    secret means the container must not start. The secret is the ASCII HMAC key
    and is never logged or framed.
    """
    secret = os.environ.get(EXECUTION_SECRET_ENV)
    port_text = os.environ.get(EXECUTION_PORT_ENV)
    os.environ.pop(EXECUTION_SECRET_ENV, None)

    if not is_lower_hex_64(secret):
        raise RuntimeError("invalid Relay execution configuration")

    if (
        not isinstance(port_text, str)
        or not port_text.isascii()
        or not port_text.isdigit()
        or not 1 <= int(port_text) <= 65535
    ):
        raise RuntimeError("invalid Relay execution configuration")

    return ExecutionConfig(secret=secret, port=int(port_text))
