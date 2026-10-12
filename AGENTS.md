# airdrop_checker — agent notes

A single-process loop. It reads the `Wallets` table of a Grist document, picks wallets that
have an address but no HYPE values yet, asks three purrfolio.com endpoints how much HYPE
each one holds (through a rotating proxy), writes the two numbers back, and sleeps. The
loop's own parameters (proxy string, wallets per round, sleep time) live in the `Settings`
table of the same document. No HTTP, no port, no state of its own.

Production: the `airdropchecker` container in the `crypt-common` stack on nebula, following
`gitea.vvzvlad.xyz/projects/airdrop_checker:latest` (watchtower + auto-heal labels).

## Layout

```
main.py                 thin entry point; `from src.checker import run`
src/settings.py         the ONLY place environment configuration is read
src/config_errors.py    turns a pydantic ValidationError into a readable startup message
src/checker.py          the loop: logger, Grist client, heartbeat, `while True`
src/balances.py         check_balance / generate_proxy / find_none_values /
                        redact_credentials / describe_error
src/heartbeat.py        the liveness mark — stdlib only
src/healthcheck.py      the Docker HEALTHCHECK probe (`python -m src.healthcheck`)
tests/                  pytest suite; offline, everything mocked
ci/smoke.py             CI gate, run inside a live container of the built image
```

## Environment — a production contract

Three required variables, no defaults: `GRIST_SERVER`, `GRIST_DOC_ID`, `GRIST_API_KEY`.
The `crypt-common` stack feeds exactly these names; renaming one takes production down.
Optional: `HEARTBEAT_FILE` (default `/tmp/airdrop_checker_heartbeat`) and
`HEARTBEAT_MAX_AGE` (default 1200 s). `.env.example` has placeholders; `make env` copies it.
A missing variable exits 1 with a message naming it.

- Nothing outside `src/settings.py` reads configuration via `os.getenv`, except
  `src/healthcheck.py`, which must not import `src.settings` (a missing credential would make
  the probe report a configuration problem instead of the heartbeat verdict).
- Set `HEARTBEAT_FILE` in the environment, never in `.env`: the loop reads `.env`, the probe
  does not, and the two would watch different paths.

## Grist names you must not "fix"

The document belongs to someone else and `get_setting` raises on an unknown name:

- Tables: `Wallets`, `Settings`.
- Settings rows: `Proxy`, `Walled count max`, `Walled count min`, `Wait time max`,
  `Wait time min` ("Walled" is a typo in the document itself).
- Wallet columns read: `Address`, `hypercore_hype_value`, `hyperevm_hype_value`; written on
  success: `hypercore_hype_value`, `hyperevm_hype_value`.

Known risk, left for the document owner to decide: the per-wallet error handler writes `Value`
and `Comment`. If the table lacks them, Grist answers 400 inside the `except` block and the
round dies on the outer handler. Do not change it silently; the tests deliberately reach the
round handler through `fail_generate_proxy` so they do not depend on those columns.

## Heartbeat and HEALTHCHECK

`src/healthcheck.py` exits 0 while `HEARTBEAT_FILE` is younger than `HEARTBEAT_MAX_AGE`. The
loop marks before the first Grist call, at the top of each iteration, after the settings and
wallet fetch, at the start of every wallet, and after every 30 s slice of every sleep.
`sleep_with_heartbeat()` is the only way the loop sleeps: the pause comes from Grist in
minutes and the round size is the operator's number, so a long pause and a long round must
both read as healthy. `tests/test_checker.py` pins each mark by position.

The Portainer deploy waits for `healthy` within `max(120s, start_period + 15s)` and rolls
back otherwise, hence `--start-period=120s` and the first mark before any network call.

The CMD is a direct `python main.py`; do not wrap it in a shell restart loop (restarting is
`restart: unless-stopped`'s job). `ci/smoke.py` fails if PID 1 is not python running `main.py`.

## Non-root

The image installs `gosu`, creates `app` (uid 1000) and has no `USER` directive: the
entrypoint starts as root, chowns `/app/data` and drops to `app`, respecting a compose `user:`
override. The probe drops privileges itself (docker runs HEALTHCHECK outside the ENTRYPOINT);
a failed drop is reported on stderr, never fatal. The privilege primitives are module-level
wrappers so tests patch them instead of the stdlib `os`; the real switch is only exercised by
`ci/smoke.py` group (l).

## Proxy credentials

The `Proxy` setting carries `user:password@`, and requests/urllib3 quote it in exception text.
Everything leaving the process (logs, the `Comment` cell, tracebacks) goes through
`redact_credentials`; it is a best-effort regex that prefers over-redaction. `check_balance`
re-raises `from None` so the unredacted `__cause__` never renders. Reasons are built by
`describe_error` (`ClassName: redacted text`), since many caught exceptions stringify to ''.

## Tests and CI

`make install`, `make test`, `make run` (needs a filled `.env`), `make clean`. Tests run in CI,
not on the laptop. Everything is offline; `tests/conftest.py` seeds the three variables.

`.gitea/workflows/`:

- `tests.yml` — PR gate: `test` → `build-and-smoke`. Never logs in, never pushes, no secrets.
- `image-check-publish.yml` — push to `main`: `test` → build → smoke → login → push `:<sha>`
  then `:latest`.

Rules for both files:

- the suite runs inside `python:3.9-slim` (the image's interpreter) with the tree streamed in
  as a tar; no `actions/setup-python`, no bind mount. `requirements-dev.txt` pins pytest 8.4.x
  because pytest 9 needs python ≥ 3.10;
- smoke sits between build and push, and `docker login` comes after it;
- no published ports; containers are reached with `docker exec`;
- no `${{ ... }}` inside a `run:` body — values arrive through `env:`;
- every CI container is named, and every removal carries `-v`;
- the shared `run:` bodies (test, smoke, cleanups) are byte-identical between the files;
  compare them mechanically after a change.

`ci/smoke.py` never uses `assert`, checks every target before reporting, and counts its
verdicts against `EXPECTED_TARGETS`. On a mismatch, find the missing check; do not edit the
number. The workflow waits for docker's own health verdict and fails on an empty one.

## Dependencies

`requirements.txt` is a fully pinned closure. `PySocks` is not imported by name but is needed
by `requests` for `socks5://` proxies. The pydantic stack is pinned to versions with cp39
manylinux wheels. The base is `python:3.9-slim` (end-of-life line).

## Conventions

- Comments, log lines and error messages in English.
- No real secrets in the repository; `.env` is gitignored and kept out of the image.
- Prefer a `make` target over a one-off shell command.
- Production behaviour is a contract: preserve it, or say explicitly that you change it.

## Limits for agents
- Tests: at most three times the code (`wc -l` of `tests/` against the code). Over the limit,
  still write the test you need, and in the same change go through the old tests: drop or merge
  duplicates and tests of implementation rather than behaviour.
- Comments explain the code as it is and why it is built this way. History ("used to", "after
  the review", "since the incident") goes into the commit message.
- This file stays under ~150 lines: only what an agent would get wrong without it. No decision
  history, incident logs or retelling of the code.
