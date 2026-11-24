import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { createServer } from "node:net";

let executionSecret;
const MAX_HANDSHAKE_BYTES = 4 * 1024;
const MAX_INVOKE_BYTES = 8 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 1024 * 1024;
const HANDSHAKE_TIMEOUT_MS = 2000;
const MAX_PENDING_HANDSHAKES = 8;
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

class ProtocolError extends Error {}

class ChunkReader {
  constructor(socket) {
    this.iterator = socket[Symbol.asyncIterator]();
    this.chunk = null;
    this.offset = 0;
  }

  async readExact(size, allowCleanEnd = false) {
    const result = Buffer.allocUnsafe(size);
    let received = 0;

    while (received < size) {
      if (this.chunk === null || this.offset === this.chunk.length) {
        const next = await this.iterator.next();
        if (next.done) {
          if (received === 0 && allowCleanEnd) {
            return null;
          }

          throw new ProtocolError("incomplete frame");
        }

        this.chunk = next.value;
        this.offset = 0;

        continue;
      }

      const length = Math.min(size - received, this.chunk.length - this.offset);
      this.chunk.copy(result, received, this.offset, this.offset + length);
      this.offset += length;
      received += length;
    }

    return result;
  }
}

async function readFrame(reader, limit, allowCleanEnd = false) {
  const header = await reader.readExact(4, allowCleanEnd);

  if (header === null) {
    return null;
  }

  const size = header.readUInt32BE(0);

  if (size === 0 || size > limit) {
    throw new ProtocolError("invalid frame size");
  }

  const payload = await reader.readExact(size);
  let text;
  try {
    text = utf8Decoder.decode(payload);
    return JSON.parse(text);
  } catch {
    throw new ProtocolError("invalid JSON frame");
  }
}

async function writeFrame(socket, value, limit) {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  if (payload.length === 0 || payload.length > limit) {
    throw new ProtocolError("response frame too large");
  }
  const frame = Buffer.allocUnsafe(payload.length + 4);
  frame.writeUInt32BE(payload.length, 0);
  payload.copy(frame, 4);
  await new Promise((resolve, reject) => {
    socket.write(frame, (error) => (error ? reject(error) : resolve()));
  });
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function parseInvoke(value) {
  if (
    !isObject(value) ||
    value.type !== "invoke" ||
    typeof value.id !== "string" ||
    value.id.length === 0 ||
    typeof value.handler !== "string" ||
    (value.env !== null && !isObject(value.env)) ||
    !Object.hasOwn(value, "event") ||
    !isObject(value.context)
  ) {
    throw new ProtocolError("invalid invocation request");
  }
  const contextFields = [
    "requestId",
    "attempt",
    "maxAttempts",
    "deadline",
    "app",
    "handler",
  ];
  if (
    contextFields.some((field) => !(field in value.context)) ||
    value.context.handler !== value.handler ||
    typeof value.context.requestId !== "string" ||
    !Number.isInteger(value.context.attempt) ||
    !Number.isInteger(value.context.maxAttempts) ||
    typeof value.context.deadline !== "string" ||
    typeof value.context.app !== "string" ||
    (Object.hasOwn(value.context, "messageId") &&
      typeof value.context.messageId !== "string") ||
    (Object.hasOwn(value.context, "traceId") &&
      typeof value.context.traceId !== "string")
  ) {
    throw new ProtocolError("invalid invocation context");
  }
  if (value.trace !== undefined && !isObject(value.trace)) {
    throw new ProtocolError("invalid trace carrier");
  }
  if (
    Buffer.byteLength(
      JSON.stringify({ type: "result", id: value.id, ok: false, error: "" }),
      "utf8",
    ) > MAX_RESPONSE_BYTES
  ) {
    throw new ProtocolError("invocation id exceeds response limit");
  }
  return value;
}

function macFor(prefix, nonce) {
  return createHmac("sha256", Buffer.from(executionSecret, "ascii"))
    .update(prefix + nonce, "ascii")
    .digest();
}

function validMac(candidate, expected) {
  if (typeof candidate !== "string" || !/^[0-9a-f]{64}$/.test(candidate)) {
    return false;
  }
  return timingSafeEqual(Buffer.from(candidate, "hex"), expected);
}

export async function runProtocol(invoke) {
  const executionSecretValue = process.env.RELAY_EXECUTION_SECRET;
  const executionPortValue = process.env.RELAY_EXECUTION_PORT;
  delete process.env.RELAY_EXECUTION_SECRET;
  if (
    !/^[0-9a-f]{64}$/.test(executionSecretValue ?? "") ||
    !/^\d+$/.test(executionPortValue ?? "")
  ) {
    throw new Error("invalid Relay execution configuration");
  }
  executionSecret = executionSecretValue;
  const executionPort = Number(executionPortValue);
  if (
    !Number.isInteger(executionPort) ||
    executionPort < 1 ||
    executionPort > 65535
  ) {
    throw new Error("invalid Relay execution configuration");
  }

  let server;
  let activeSocket = null;
  let shuttingDown = false;
  let pendingHandshakes = 0;
  const sockets = new Set();

  function stop(exitCode) {
    if (shuttingDown) return;
    shuttingDown = true;
    process.exitCode = exitCode;
    if (server.listening) server.close();
    for (const socket of sockets) socket.destroy();
  }

  async function serve(socket, onAuthenticated) {
    socket.setNoDelay(true);
    const reader = new ChunkReader(socket);
    const handshakeTimer = setTimeout(
      () => socket.destroy(),
      HANDSHAKE_TIMEOUT_MS,
    );
    try {
      const hello = await readFrame(reader, MAX_HANDSHAKE_BYTES);
      if (
        !isObject(hello) ||
        hello.type !== "hello" ||
        hello.version !== 1 ||
        typeof hello.nonce !== "string" ||
        !/^[0-9a-f]{64}$/.test(hello.nonce)
      ) {
        throw new ProtocolError("invalid handshake");
      }

      const nonce = randomBytes(32).toString("hex");
      await writeFrame(
        socket,
        {
          type: "auth",
          version: 1,
          mac: macFor("relay-bootstrap-auth-v1:", hello.nonce).toString("hex"),
          nonce,
        },
        MAX_HANDSHAKE_BYTES,
      );

      const ready = await readFrame(reader, MAX_HANDSHAKE_BYTES);
      if (
        !isObject(ready) ||
        ready.type !== "ready" ||
        ready.version !== 1 ||
        !validMac(ready.mac, macFor("relay-client-auth-v1:", nonce))
      ) {
        throw new ProtocolError("invalid authentication");
      }

      if (activeSocket !== null) {
        throw new ProtocolError("connection already active");
      }
      activeSocket = socket;
      clearTimeout(handshakeTimer);
      onAuthenticated();
      for (const pendingSocket of sockets) {
        if (pendingSocket !== socket) pendingSocket.destroy();
      }
      socket.once("close", () => {
        if (activeSocket === socket) stop(0);
      });

      while (!socket.destroyed) {
        const raw = await readFrame(reader, MAX_INVOKE_BYTES, true);
        if (raw === null) return;
        const req = parseInvoke(raw);
        const result = await invoke(req);
        await writeFrame(socket, result, MAX_RESPONSE_BYTES);
      }
    } finally {
      clearTimeout(handshakeTimer);
    }
  }

  server = createServer((socket) => {
    if (
      shuttingDown ||
      activeSocket !== null ||
      pendingHandshakes >= MAX_PENDING_HANDSHAKES
    ) {
      socket.destroy();
      return;
    }
    pendingHandshakes++;
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    let authenticated = false;
    serve(socket, () => {
      authenticated = true;
      pendingHandshakes--;
    })
      .catch(() => {
        if (activeSocket === socket) stop(1);
        else socket.destroy();
      })
      .finally(() => {
        if (!authenticated) pendingHandshakes--;
        if (activeSocket === socket) {
          activeSocket = null;
          stop(0);
        }
      });
  });

  server.on("error", () => {
    stop(1);
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(executionPort, "0.0.0.0", resolve);
  });
}
