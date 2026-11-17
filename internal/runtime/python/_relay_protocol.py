"""Relay's authenticated, length-prefixed JSON execution-channel protocol.

Embedded beside the bootstrap and written to ``/relay/_relay_protocol.py`` in
the runtime image.

Wire format (both directions):
  4-byte big-endian unsigned payload length, then that many UTF-8 JSON bytes.
  There is no newline or sentinel. Handshake frames are capped at 4 KiB, invoke
  requests at 8 MiB, and results at 1 MiB. A zero or oversized length is
  rejected before allocation; reads are exact and writes complete.

Handshake (once per connection, before any invocation):
  Relay    -> {"type":"hello","version":1,"nonce":"<64 lowercase hex>"}
  bootstrap-> {"type":"auth","version":1,"mac":"<64 lowercase hex>",
               "nonce":"<64 lowercase hex>"}
  Relay    -> {"type":"ready","version":1,"mac":"<64 lowercase hex>"}
  Both MACs are HMAC-SHA256 keyed by the ASCII bytes of RELAY_EXECUTION_SECRET
  (64 lowercase hex chars): auth signs "relay-bootstrap-auth-v1:" plus the
  Relay nonce, ready signs "relay-client-auth-v1:" plus the bootstrap nonce.
  The bootstrap sends auth only once it is ready to read invocations and
  verifies ready with hmac.compare_digest before accepting an invoke frame.
  Unauthenticated peers are closed by the server after a short deadline without
  running a handler; the listener stays available for Relay.

Invocation:
  Relay    -> {"type":"invoke","id":"<id>","handler":"mod.func",
               "event":<raw JSON>,"env":{"K":"V"},
               "trace":{"traceparent":"...","tracestate":"...",
                        "baggage":"..."},
               "context":{"requestId":"<id>","attempt":1,
                          "maxAttempts":3,"deadline":"<ISO-8601>",
                          "messageId":"<optional>","traceId":"<optional>",
                          "app":"<app>","handler":"mod.func"}}
  bootstrap-> {"type":"result","id":"<same id>","ok":true}
              {"type":"result","id":"<same id>","ok":false,"error":"..."}
  The id is echoed exactly. "trace" is omitted when Relay has no trace context;
  "context" is always present. Error text is bounded so the serialized result
  stays within the 1 MiB response cap (and never exceeds 64 KiB), with
  truncation on a Unicode code-point boundary.
"""

import hashlib
import hmac
import json
import secrets
import struct

# Relay's execution-channel contract (see internal/runtime/protocol.go in the
# Relay source). The limits bound the JSON payload only, excluding the 4-byte
# length header.
PROTOCOL_VERSION = 1
MAX_HANDSHAKE_BYTES = 4 * 1024
MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_RESPONSE_BYTES = 1 * 1024 * 1024
MAX_ERROR_BYTES = 64 * 1024
BOOTSTRAP_AUTH_CONTEXT = "relay-bootstrap-auth-v1:"
CLIENT_AUTH_CONTEXT = "relay-client-auth-v1:"


class ProtocolError(Exception):
    """A fatal violation of the authenticated execution-channel protocol."""


def is_lower_hex_64(value):
    """Whether value is the protocol's 64-character lowercase hex form."""
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(ch in "0123456789abcdef" for ch in value)


def is_protocol_version(value):
    """Whether value is the integer protocol version. JSON true compares equal
    to 1 in Python but is not a version and must be rejected."""
    return type(value) is int and value == PROTOCOL_VERSION


def dumps(value):
    """Serialize one frame payload without incidental whitespace."""
    return json.dumps(value, separators=(",", ":"))


def _json_escaped_bytes(ch):
    """Number of UTF-8 bytes one Unicode character contributes inside a JSON
    string (its escaped form, without the surrounding quotes). JSON escaping is
    per-character, so summing these is exactly the error field's serialized
    contribution."""
    return len(json.dumps(ch)[1:-1].encode("utf-8"))


def _bounded_error(req_id, message):
    """Return an error string such that the whole serialized result payload
    fits in MAX_RESPONSE_BYTES and the error itself stays within MAX_ERROR_BYTES,
    while always remaining valid UTF-8.

    The budget is the smaller of the diagnostic cap and the response cap minus
    the exact size of the envelope with an empty error. The message is then
    accumulated one Unicode character at a time (including any JSON escaping of
    quotes, backslashes, and control characters), so the payload is bounded
    precisely, a multibyte character is never cut mid-byte, and the result always
    round-trips through json.dumps (the emitted frame is parseable).
    """
    overhead = len(
        dumps({"type": "result", "id": req_id, "ok": False, "error": ""}).encode(
            "utf-8"
        )
    )
    budget = min(MAX_ERROR_BYTES, MAX_RESPONSE_BYTES - overhead)
    if budget <= 0:
        return ""
    parts = []
    used = 0
    for ch in message:
        n = _json_escaped_bytes(ch)
        if used + n > budget:
            break
        parts.append(ch)
        used += n
    return "".join(parts)


def result_payload(req_id, ok, error=None):
    """Build one result payload, bounding the error and enforcing the response
    cap. A result that cannot fit is a protocol failure: Relay would reject an
    oversized frame anyway."""
    if ok:
        frame = {"type": "result", "id": req_id, "ok": True}
    else:
        frame = {
            "type": "result",
            "id": req_id,
            "ok": False,
            "error": _bounded_error(req_id, "" if error is None else str(error)),
        }
    payload = dumps(frame).encode("utf-8")
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ProtocolError("result payload exceeds the response frame limit")
    return payload


async def read_frame(reader, max_bytes):
    """Read one length-prefixed frame, rejecting zero and oversized lengths
    before allocating, and requiring the complete payload."""
    header = await reader.readexactly(4)
    (size,) = struct.unpack(">I", header)
    if size == 0:
        raise ProtocolError("empty frame")
    if size > max_bytes:
        raise ProtocolError(
            f"frame of {size} bytes exceeds the {max_bytes}-byte limit"
        )
    return await reader.readexactly(size)


async def write_frame(writer, payload, max_bytes):
    """Write one length-prefixed frame completely, then wait for the transport
    to drain its buffer."""
    if not payload:
        raise ProtocolError("empty frame")
    if len(payload) > max_bytes:
        raise ProtocolError(
            f"frame of {len(payload)} bytes exceeds the {max_bytes}-byte limit"
        )
    writer.write(struct.pack(">I", len(payload)) + payload)
    await writer.drain()


def _reject_json_constant(value):
    """Reject JSON extensions Python accepts but the wire contract (and
    JSON.parse) does not: NaN, Infinity, and -Infinity."""
    raise ValueError(f"non-standard JSON constant {value}")


def decode_json(payload):
    """Decode one UTF-8 JSON frame payload, rejecting malformed input and
    non-standard constants before any handler can run."""
    try:
        return json.loads(payload.decode("utf-8"), parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError(f"invalid JSON frame: {exc}") from exc


def challenge_mac(secret, auth_context, nonce):
    """Compute one handshake MAC in the protocol's lowercase hex form. The key
    is the ASCII secret and the message is the direction-specific context prefix
    plus the nonce exactly as sent."""
    message = (auth_context + nonce).encode("ascii")
    return hmac.new(secret.encode("ascii"), message, hashlib.sha256).hexdigest()


async def handshake(reader, writer, secret):
    """Authenticate Relay and leave the connection ready for invoke frames.

    The bootstrap answers Relay's hello with its own nonce and MAC, then requires
    Relay to prove knowledge of the secret over the bootstrap nonce before any
    handler can run. Both MAC checks use constant-time comparison. Raise
    ProtocolError (or an I/O error) on any mismatch or unexpected frame.
    """
    hello = decode_json(await read_frame(reader, MAX_HANDSHAKE_BYTES))
    if (
        not isinstance(hello, dict)
        or hello.get("type") != "hello"
        or not is_protocol_version(hello.get("version"))
    ):
        raise ProtocolError("unexpected bootstrap hello frame")
    relay_nonce = hello.get("nonce")
    if not is_lower_hex_64(relay_nonce):
        raise ProtocolError("malformed Relay nonce")

    bootstrap_nonce = secrets.token_hex(32)
    auth = {
        "type": "auth",
        "version": PROTOCOL_VERSION,
        "mac": challenge_mac(secret, BOOTSTRAP_AUTH_CONTEXT, relay_nonce),
        "nonce": bootstrap_nonce,
    }
    await write_frame(writer, dumps(auth).encode("utf-8"), MAX_HANDSHAKE_BYTES)

    ready = decode_json(await read_frame(reader, MAX_HANDSHAKE_BYTES))
    if (
        not isinstance(ready, dict)
        or ready.get("type") != "ready"
        or not is_protocol_version(ready.get("version"))
    ):
        raise ProtocolError("unexpected Relay ready frame")
    mac = ready.get("mac")
    if not is_lower_hex_64(mac):
        raise ProtocolError("Relay authentication failed")
    expected = challenge_mac(secret, CLIENT_AUTH_CONTEXT, bootstrap_nonce)
    if not hmac.compare_digest(mac, expected):
        raise ProtocolError("Relay authentication failed")
