Environment: Xray-core-fedarisha `26.9.9-1.0.1fed` (go1.27.1, darwin/arm64), client config for the S3 tunnel, single S3 provider, macOS (Apple Silicon).

## Symptom

Under concurrent downloads through the local SOCKS the tunnel freezes while the core process stays healthy (no crash, stable RSS). The UI (any supervisor that only watches the process) keeps reporting "connected" for the whole outage.

## Reproduction (reliable, 2 out of 2 runs)

1. Start core with a normal client config.
2. Fire 6 parallel 50 MB downloads through the SOCKS:
   `curl --socks5-hostname 127.0.0.1:10808 "https://speed.cloudflare.com/__down?bytes=50000000"` x6.
3. All downloads stall within ~9 s.

## Timeline from one run

- `hole at seq 15 persisted 7.123s (5 present), closing for re-dial` -> session close with `S3 puts: 11, gets: 64, put_errs: 0, get_errs: 45`
- re-dial succeeds once
- second `hole at seq 20` -> close with `put_errs: 0, get_errs: 39`
- then 5 consecutive dials, 3 of them ending in `fedarisha dial: server did not ACK within 60s` — no `accepted by server` during this window
- total outage ~4 min, then `accepted by server` and traffic flows again

## Observations

- TCP to the rendezvous server:443 succeeds during the outage — network path is fine.
- `put_errs: 0` vs `get_errs: 39-45`: only S3 reads fail/timeout under load; writes are unaffected.
- 2 parallel downloads did not trigger a new hole while the session was already degraded, so the trigger appears to be the read path under concurrent fetch, not raw bandwidth.

## Hypotheses (not sure)

- The client-side S3 GET/poll loop stalls or times out under concurrency, leaving a yamux gap that never fills.
- S3 throttles GETs (429/5xx) under burst and the client doesn't surface those errors at INFO level (no 429/timeout lines in the log).

## Ask

Guidance on raising log verbosity to capture the actual GET errors, and/or a fix for the read-path stall + ACK handling so a degraded session fails over faster than ~4 min.

Happy to re-run with debug logging if you tell me the flag/config.
