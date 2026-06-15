// Minimal waitbus broadcast subscriber (TypeScript, Node stdlib only).
//
// Mirror of minimal_subscriber.py; wire contract documented there.
//
// Run:
//     node --experimental-strip-types minimal_subscriber.ts
//   or:
//     tsc minimal_subscriber.ts && node minimal_subscriber.js
//
// No npm dependencies; uses only Node's built-in `net`, `os`, `path`,
// `process`, and `Buffer`.

import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";

// `process` is the Node global; using the import-binding form
// (`import * as process from "node:process"`) returns the module
// namespace, which under Node's --experimental-strip-types runner does
// NOT expose the EventEmitter methods (.on, .exit) the canonical global
// does. We use the global directly. Typecheck-time it relies on
// @types/node; the snippet runs without any npm dependency.

const MAX_FRAME_BYTES = 65_536;

function defaultSocketPath(): string {
  const override = process.env.WAITBUS_BROADCAST_SOCKET;
  if (override) {
    return override;
  }
  if (process.platform === "darwin") {
    return path.join(
      os.homedir(),
      "Library",
      "Application Support",
      "waitbus",
      "broadcast.sock",
    );
  }
  const runtimeDir =
    process.env.XDG_RUNTIME_DIR ??
    `/run/user/${typeof process.getuid === "function" ? process.getuid() : 0}`;
  return path.join(runtimeDir, "waitbus", "broadcast.sock");
}

// Frame reader: parses a contiguous Buffer for length-prefixed frames.
// Emits each payload via the `onFrame` callback. Returns leftover bytes
// that did not yet form a complete frame.
function consumeFrames(
  buffer: Buffer,
  onFrame: (payload: Buffer) => void,
): Buffer {
  let offset = 0;
  while (buffer.length - offset >= 4) {
    const length = buffer.readUInt32BE(offset);
    if (length === 0 || length > MAX_FRAME_BYTES) {
      throw new Error(`frame length ${length} out of bounds`);
    }
    if (buffer.length - offset - 4 < length) {
      break;
    }
    const payload = buffer.subarray(offset + 4, offset + 4 + length);
    onFrame(payload);
    offset += 4 + length;
  }
  return buffer.subarray(offset);
}

function writeFrame(socket: net.Socket, payload: Buffer): void {
  if (payload.length > MAX_FRAME_BYTES) {
    throw new Error(
      `payload ${payload.length} bytes exceeds ${MAX_FRAME_BYTES}`,
    );
  }
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32BE(payload.length, 0);
  socket.write(prefix);
  socket.write(payload);
}

interface EventFrame {
  kind?: string;
  event_id?: string;
  delivery_id?: string;
  event_type?: string;
  fields?: { source?: string };
  reason?: string;
  remediation?: string;
}

function handleFrame(payload: Buffer): void {
  let event: EventFrame;
  try {
    event = JSON.parse(payload.toString("utf-8")) as EventFrame;
  } catch (err) {
    process.stderr.write(`error: invalid JSON frame: ${String(err)}\n`);
    process.exit(1);
    return;
  }
  if (event.kind === "subscribe_rejected") {
    const reason = event.reason ?? "unknown";
    process.stderr.write(`error: subscribe_rejected: ${reason}\n`);
    if (event.remediation) {
      process.stderr.write(`remediation: ${event.remediation}\n`);
    }
    process.exit(2);
    return;
  }
  // A "truncated" frame is a DATA frame (it carries an event_id and
  // advances the resume cursor), not a control frame: the event's payload
  // exceeded the wire cap, so only its identity rides the socket. Surface
  // it -- silently dropping it makes a large event invisible -- and
  // re-fetch the full row out of band.
  if (event.kind === "truncated") {
    process.stdout.write(`${event.event_id ?? "?"}\t[truncated; re-fetch full payload via \`waitbus read-events\`]\n`);
    return;
  }
  // Control frames (daemon_heartbeat, subscribe_ack) carry no event
  // identity; skip them.
  if (event.kind !== "event") {
    return;
  }
  const deliveryId = event.delivery_id ?? "?";
  const eventType = event.event_type ?? "?";
  const source = event.fields?.source ?? "?";
  process.stdout.write(`${deliveryId}\tsource=${source}\ttype=${eventType}\n`);
}

function main(): void {
  const socketPath = defaultSocketPath();
  const socket = net.createConnection(socketPath);

  let buffer: Buffer = Buffer.alloc(0);
  let connected = false;

  socket.on("connect", () => {
    connected = true;
    // Subscribe envelope: proto=1 is mandatory. Empty filters means "all
    // repos, all event types, from now". Add "filters" or "event_types"
    // keys to narrow.
    writeFrame(socket, Buffer.from('{"proto":1}', "utf-8"));
  });

  socket.on("data", (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);
    try {
      buffer = consumeFrames(buffer, handleFrame);
    } catch (err) {
      process.stderr.write(`error: ${String(err)}\n`);
      socket.destroy();
      process.exit(1);
    }
  });

  socket.on("error", (err: NodeJS.ErrnoException) => {
    if (!connected) {
      process.stderr.write(
        `error: broadcast socket ${socketPath} unavailable (${err.code ?? err.message}). ` +
          "Start the daemon via `systemctl --user start waitbus-broadcast.service`.\n",
      );
      process.exit(2);
    }
    process.stderr.write(`error: ${err.message}\n`);
    process.exit(1);
  });

  socket.on("end", () => {
    process.exit(0);
  });

  socket.on("close", () => {
    process.exit(0);
  });

  process.on("SIGINT", () => {
    socket.destroy();
    process.exit(0);
  });
}

main();
