# @mastra/core durable agent: auto-cleanup leaks one pubsub subscription per `stream()` run

Minimal reproduction. Stock `EventEmitterPubSub` + `LibSQLStore` + `MockLanguageModelV3`, no transport, no app code.

```sh
npm i
npm run leak        # 20 runs -> 20 subscriptions still attached after auto-cleanup
npm run weakref     # 30 runs -> 30 output objects still reachable after forced GC
npm run untilidle   # same via stream(..., { untilIdle })
```

Expected output of `npm run leak` on `@mastra/core@1.67.0`, Node 24:

```
stream() runs executed .......................... 20
  ...still subscribed after auto-cleanup ........ 20
cleanup() called BEFORE the timer ............... released
cleanup() called AFTER the timer ................ LEAKED (no-op)
generate() ...................................... released

live agent.stream.*  topics on the emitter ...... 21
live agent.control.* topics on the emitter ...... 0   <- torn down correctly
```

Cause: in `DurableAgent.stream()` the auto-cleanup timer calls `#clearPubsubTopic(runId)` but never the stream's own `cleanup()` (`streamCleanup`), so the `agent.stream.<runId>` subscriber stays attached for the life of the process. `observe()` already routes both paths through `performCleanup()`, which does call `streamCleanup?.()`.
