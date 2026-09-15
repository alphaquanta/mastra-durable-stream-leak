import EventEmitter from "node:events";
import { Agent } from "@mastra/core/agent";
import { EventedAgent } from "@mastra/core/agent/durable";
import { EventEmitterPubSub } from "@mastra/core/events";
import { Mastra } from "@mastra/core/mastra";
import { LibSQLStore } from "@mastra/libsql";
import { MockLanguageModelV3, simulateReadableStream } from "ai/test";

const CLEANUP_MS = 1_000;
const model = new MockLanguageModelV3({
  doStream: async () => ({ stream: simulateReadableStream({ chunks: [
    { type: "stream-start", warnings: [] },
    { type: "text-start", id: "0" }, { type: "text-delta", id: "0", delta: "hi" }, { type: "text-end", id: "0" },
    { type: "finish", finishReason: "stop", usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }] }) }),
});
const emitter = new EventEmitter(); emitter.setMaxListeners(0);
const agent = new Agent({ id: "leaky", name: "leaky", instructions: "t", model });
const durable = new EventedAgent({ agent, cleanupTimeoutMs: CLEANUP_MS });
const mastra = new Mastra({
  storage: new LibSQLStore({ id: "r", url: ":memory:" }),
  pubsub: new EventEmitterPubSub(emitter),
  agents: { leaky: durable }, logger: false,
});
await mastra.startWorkers();

// A chat runtime that starts each turn with untilIdle leaks the same way
for (let i = 0; i < 5; i++) {
  const r = await durable.stream("hello", { untilIdle: { maxIdleMs: 300 } });
  for await (const _ of r.output.fullStream) { /* drain */ }
}
await new Promise((r) => setTimeout(r, CLEANUP_MS + 2_000));
const topics = emitter.eventNames().filter((n) => String(n).startsWith("agent.stream."));
console.log(`untilIdle runs: 5 | leaked agent.stream.* topics: ${topics.length}`);
await mastra.shutdown(); process.exit(0);
