# hive-dvergr

Experimental open-source adapter between [dvergr](https://github.com/replikativ/dvergr)
and Hive agent runtimes.

The project implements `hive-addon.protocol/IAddon`, wraps dvergr's
`GenerationHandle` behind a host-neutral run port, and commits full run evidence
to Datahike before publishing lifecycle events.

## Experiment boundary

This spike tests:

- dvergr execution handles as a Hive run runtime;
- durable, task-correlated Datahike run evidence;
- process-independent result recovery;
- cancellation that reaches a durable terminal state even when dvergr's
  best-effort future cancellation never resolves `GenerationHandle.done`;
- optional adaptation to Hive's current `IAgenticLoop` and headless backend
  surfaces without a production dependency on `hive-mcp`.

It does **not** yet claim complete replacement of Datalevin in `hive-agent`.
Hive transcripts already support a Datahike backend, but the conversational KG
still has Datalevin-only production paths. The current Datahike transcript
`fork-from-cursor` also records a cursor without applying it to reads.

## Architecture

```text
dvergr GenerationHandle
        |
        v
IRunRuntime -> DatahikeRunLedger -> optional lifecycle publisher
        |
        +-> dynamic Hive IAgenticLoop adapter (when host protocols exist)

HiveDvergrAddon
        +-> :ag/run-runtime
        +-> :ag/loop-factory
        +-> :ag/loop-backend
```

Datahike is authoritative for run identity and outcomes. Preview events may be
truncated by a host UI, but artifact/result data in the ledger is not.

## Development

```bash
clojure -M:local-src:test
```

The dvergr dependency is pinned to an exact Git SHA. `:local-src` replaces it
with `../clones-ref/dvergr` for joint development.

`hive-agent` integration is deliberately optional:

```bash
clojure -M:local-src:hive-agent-local:test
```

## License

MIT
