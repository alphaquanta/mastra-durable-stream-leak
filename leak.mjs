// Reproduction: @mastra/core durable agent leaks one pubsub subscription per run.
//
// Everything here is stock @mastra/core: the built-in EventEmitterPubSub,
// LibSQLStore, and a mock model. No third-party transport, no app code.
//
//   npm i @mastra/core@1.67.0 @mastra/libsql ai && node leak.mjs

import EventEmitter from "node:events";
import { Agent } from "@mastra/core/agent";
import { EventedAgent } from "@mastra/core/agent/durable";
import { EventEmitterPubSub } from "@mastra/core/events";
import { Mastra } from "@mastra/core/mastra";
import { LibSQLStore } from "@mastra/libsql";
import { MockLanguageModelV3, simulateReadableStream } from "ai/test";

const CLEANUP_MS = 1_000;        // default is 30_000; shortened to keep this fast
const RUNS = 20;

const model = new MockLanguageModelV3({
  doStream: async () => ({
    stream: simulateReadableStream({
      chunks: [
        { type: "stream-start", warnings: [] },
        { type: "text-start", id: "0" },
        { type: "text-delta", id: "0", delta: "hi" },
        { type: "text-end", id: "0" },
        { type: "finish", finishReason: "stop",
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
      ],
    }),
  }),
});

const emitter = new EventEmitter();
emitter.setMaxListeners(0);

const agent = new Agent({ id: "leaky", name: "leaky", instructions: "t", model });
const durable = new EventedAgent({ agent, cleanupTimeoutMs: CLEANUP_MS });

const mastra = new Mastra({
  storage: new LibSQLStore({ id: "repro", url: ":memory:" }),
  pubsub: new EventEmitterPubSub(emitter),
  agents: { leaky: durable },
  logger: false,
});
await mastra.startWorkers();

const attached = (id) => emitter.listenerCount(`agent.stream.${id}`) > 0;
const settle = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- 1. stream(), let auto-cleanup handle it -------------------------------
const streamed = [];
for (let i = 0; i < RUNS; i++) {
  const r = await durable.stream("hello");
  streamed.push(r);
  for await (const _ of r.output.fullStream) { /* drain to completion */ }
}
await settle(CLEANUP_MS + 1_500);   // well past the auto-cleanup timer
const leaked = streamed.filter((r) => attached(r.runId));

// ---- 2. control: call the returned cleanup() BEFORE the timer fires --------
const early = await durable.stream("hello");
for await (const _ of early.output.fullStream) { /* drain */ }
await early.cleanup();
await settle(200);
const earlyLeaked = attached(early.runId);

// ---- 3. control: call cleanup() AFTER the timer already fired --------------
const late = await durable.stream("hello");
for await (const _ of late.output.fullStream) { /* drain */ }
await settle(CLEANUP_MS + 1_500);
await late.cleanup();               // no-op: `cleanedUp` is already true
await settle(200);
const lateLeaked = attached(late.runId);

// ---- 4. generate() ---------------------------------------------------------
const gen = await durable.generate("hello");
await settle(CLEANUP_MS + 1_500);
const genLeaked = attached(gen.runId);

const total = emitter.eventNames()
  .filter((n) => String(n).startsWith("agent.stream.")).length;
const controls = emitter.eventNames()
  .filter((n) => String(n).startsWith("agent.control.")).length;

console.log(`
stream() runs executed .......................... ${RUNS}
  ...still subscribed after auto-cleanup ........ ${leaked.length}
cleanup() called BEFORE the timer ............... ${earlyLeaked ? "LEAKED" : "released"}
cleanup() called AFTER the timer ................ ${lateLeaked ? "LEAKED (no-op)" : "released"}
generate() ...................................... ${genLeaked ? "LEAKED" : "released"}

live agent.stream.*  topics on the emitter ...... ${total}
live agent.control.* topics on the emitter ...... ${controls}   <- torn down correctly
`);

await mastra.shutdown();
process.exit(0);
