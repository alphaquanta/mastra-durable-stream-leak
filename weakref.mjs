// Proof of retention: does the run's output object stay reachable after auto-cleanup?
import EventEmitter from "node:events";
import { Agent } from "@mastra/core/agent";
import { EventedAgent } from "@mastra/core/agent/durable";
import { EventEmitterPubSub } from "@mastra/core/events";
import { Mastra } from "@mastra/core/mastra";
import { LibSQLStore } from "@mastra/libsql";
import { MockLanguageModelV3, simulateReadableStream } from "ai/test";

const CLEANUP_MS = 500, RUNS = 30;   // npm i @mastra/core@1.67.0 @mastra/libsql ai && node --expose-gc weakref.mjs
const model = new MockLanguageModelV3({ doStream: async () => ({ stream: simulateReadableStream({ chunks: [
  { type: "stream-start", warnings: [] }, { type: "text-start", id: "0" },
  { type: "text-delta", id: "0", delta: "x".repeat(20000) }, { type: "text-end", id: "0" },
  { type: "finish", finishReason: "stop", usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }] }) }) });
const emitter = new EventEmitter(); emitter.setMaxListeners(0);
const agent = new Agent({ id: "a", name: "a", instructions: "t", model });
const durable = new EventedAgent({ agent, cleanupTimeoutMs: CLEANUP_MS });
const mastra = new Mastra({ storage: new LibSQLStore({ id: "r", url: ":memory:" }), pubsub: new EventEmitterPubSub(emitter), agents: { a: durable }, logger: false });
await mastra.startWorkers();
const settle = (ms) => new Promise((r) => setTimeout(r, ms));
const gc = async () => { for (let i = 0; i < 5; i++) { global.gc(); await settle(50); } };

async function batch(label, explicitCleanup) {
  const refs = [];
  for (let i = 0; i < RUNS; i++) {
    let r = await durable.stream("hello");
    for await (const _ of r.output.fullStream) {}
    refs.push({ id: r.runId, out: new WeakRef(r.output) });
    if (explicitCleanup) await r.cleanup();
    r = null;
  }
  await settle(CLEANUP_MS + 800);
  await gc();
  const aliveOut = refs.filter((x) => x.out.deref() !== undefined).length;
  const listeners = refs.filter((x) => emitter.listenerCount(`agent.stream.${x.id}`) > 0).length;
  console.log(`${label.padEnd(34)} listeners=${listeners}/${RUNS}  output objects still reachable after GC=${aliveOut}/${RUNS}  heapUsed=${(process.memoryUsage().heapUsed/1048576).toFixed(1)}MB`);
}
await gc(); console.log(`baseline heapUsed=${(process.memoryUsage().heapUsed/1048576).toFixed(1)}MB`);
await batch("auto-cleanup only (the bug)", false);
await batch("explicit cleanup() before timer", true);
await mastra.shutdown(); process.exit(0);
