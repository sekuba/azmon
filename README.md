# azmon

`azmon.sh` is a shell-first host monitor for an Aztec sequencer node. It watches the `aztec-sequencer` Docker container, discovers upcoming duties for configured sequencer addresses, scans confirmed `CheckpointProposed` / `CheckpointInvalidated` events on L1, and sends Telegram alerts with local cooldown-based dedupe.

## Requirements

- `bash`
- `docker`
- `curl`
- `jq`
- `cast`

## Configuration

Copy `.env.example` into your preferred env file and set:

- `AZMON_TELEGRAM_BOT_TOKEN`
- `AZMON_TELEGRAM_CHAT_ID`
- `AZMON_MONITORED_SEQUENCERS`

## Run

```bash
./azmon.sh
```

For a local dry run without Telegram delivery:

```bash
AZMON_DRY_RUN=true ./azmon.sh
```

State is stored under `AZMON_STATE_DIR` and includes:

- `docker_cursor.txt`
- `l1_block_cursor.txt`
- `duty_cache.json`
- `completion_cache.json`
- `alert_dedupe.json`
- `runtime.json`

## Notes

- attestation completion is verified from nested rollup `propose(...)` calldata found via `debug_traceTransaction(..., {"tracer":"callTracer"})`
- attestation non-inclusion in an individual proposal is tracked but not alerted as a miss; an attestation miss is only alerted when a sequencer has zero included attestations across the entire duty epoch
- `AZMON_DUTY_INFO=true` enables full duty lifecycle info, including duty discovery, successful proposal/attestation completions, and end-of-epoch attestation stats
- if tracing fails, attestation status is kept as `unknown` and retried on later polls
