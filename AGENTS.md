# airdrop_checker — agent notes

A single-process loop. It reads the `Wallets` table of a Grist document, picks wallets that
have an address but no HYPE values yet, asks three purrfolio.com endpoints how much HYPE
each one holds (through a rotating proxy), writes the two numbers back, and sleeps for a
while. The loop's own parameters — the proxy string, how many wallets per round, how long
to sleep — live in the `Settings` table of that same document, so the operator changes them
without a redeploy.

That is the whole service. It serves no HTTP, exposes no port and keeps no state of its own.

## Structure

```
main.py                 thin entry point; `from src.checker import run`
src/settings.py         the ONLY place environment configuration is read
src/config_errors.py    turns a pydantic ValidationError into a readable startup message
src/checker.py          the loop: logger, Grist client, heartbeat, `while True`
src/grist.py            thin wrapper over grist_api (column-name and datetime translation)
src/balances.py         check_balance / generate_proxy / find_none_values / redact_credentials
src/heartbeat.py        the liveness mark — stdlib only, see below
src/healthcheck.py      the Docker HEALTHCHECK probe (`python -m src.healthcheck`)
src/http_timeout.py     global default timeout for outgoing requests
tests/                  pytest suite; offline, everything mocked
ci/smoke.py             the CI gate, run inside a live container of the built image
```

## Environment — a production contract

Exactly three variables, all REQUIRED, none with a default:

| variable | what it is |
|---|---|
| `GRIST_SERVER` | our Grist deployment holding the document |
| `GRIST_DOC_ID` | the document |
| `GRIST_API_KEY` | the key |

These names are what the `crypt-common` stack on nebula feeds the `airdropchecker`
container. **Renaming any of them takes production down.** Two optional ones,
`HEARTBEAT_FILE` and `HEARTBEAT_MAX_AGE`, have defaults that are themselves a contract (see
below). `.env.example` carries the three required ones as placeholders and the two optional
ones commented out with their default values; `make env` copies it.

`HEARTBEAT_FILE` must be overridden in the ENVIRONMENT, never in `.env`. The two sides of it
read from different places: `src/settings.py` declares `env_file=".env"`, so the loop picks
up a value written there, while `src/healthcheck.py` reads the environment only — deliberately
(see the note in that file). Set it in `.env` alone and the loop marks one path while the
probe watches another, so the container stays `unhealthy` forever. Production never sees
this, because there is no `.env` there and `ci/smoke.py` fails the build if one reaches the
image; locally it bites.

A missing variable exits(1) at startup with a message naming it. That is deliberate and is
the defect this layout fixed: the old single-file version read the three with `os.getenv`,
never checked them, and handed `None` straight to `GristDocAPI` — so an unconfigured
container died deep inside its first HTTP call with a message that named nothing.

Nothing outside `src/settings.py` may call `os.getenv` for configuration. The one exception
is `src/healthcheck.py`, and it is explained there: the probe reads the environment
directly so a missing credential cannot make it report "unhealthy" for a configuration
reason and hide the heartbeat verdict it exists to give.

## Grist names you must not "fix"

The document belongs to someone else and `find_settings` raises on a name it cannot find,
so every one of these strings is load-bearing exactly as spelled:

- Tables: `Wallets`, `Settings`.
- Settings rows: `Proxy`, **`Walled count max`**, **`Walled count min`**, `Wait time max`,
  `Wait time min`. "Walled" is a typo in the document itself. Correcting it here stops the
  service.
- Wallet columns read: `Address`, `hypercore_hype_value`, `hyperevm_hype_value`.
- Wallet columns written on success: `hypercore_hype_value`, `hyperevm_hype_value`.

`src/grist.py` rewrites spaces to underscores on the way out, because Grist accepts a
column by its identifier while the document's labels carry spaces.

## Known risk: the error path writes different columns

When one wallet fails, the handler in `src/checker.py` writes `Value` and `Comment` —
columns nothing else in this repository touches, and which the success path does not use.
If the `Wallets` table does not have them, Grist rejects the batch with 400 and that write
raises **inside the `except` block**, so the wallet's real error is replaced by an unrelated
one and the round dies on the outer handler.

This is preserved exactly as found, on purpose. Whether those columns exist is a property
of a document this repository does not own, so renaming them is the owner's decision, not a
refactor's. There is a comment at the site saying so. **Do not "fix" this silently.**

## Heartbeat, HEALTHCHECK and the sleep that must be chunked

`src/checker.py` writes a timestamp to `HEARTBEAT_FILE` (default
`/tmp/airdrop_checker_heartbeat`). `src/healthcheck.py` exits 0 while that file is younger
than `HEARTBEAT_MAX_AGE` (default 1200 s) and 1 when it is missing or stale. The Dockerfile
runs that probe as the image's `HEALTHCHECK`.

The mark is written:

1. once immediately after the settings load and the client is built, **before the first
   Grist call** — that proves the process started, and gives the container time to reach
   `healthy` inside the deploy window;
2. at the top of every loop iteration;
3. once after the settings block and the wallet selection — six HTTP round trips, all of
   them before any wallet is touched;
4. **at the start of every wallet's turn**;
5. **after every 30-second slice of every sleep.**

Points 4 and 5 are the ones that are easy to undo and expensive to lose, and they are the
same property seen from two sides: **a long round and a long pause are both normal phases of
this service, and the probe has to answer "healthy" during both.**

The pause comes from Grist in MINUTES, so a single `time.sleep(time_to_sleep)` would leave
the mark untouched for as long as somebody typed into a spreadsheet cell.
`sleep_with_heartbeat()` is therefore the ONLY way this loop sleeps, short error pauses
included, so no second path can quietly forget the mark.

The round's LENGTH is out of the code's hands in exactly the same way: `Walled count max`
in the Grist Settings table decides how many wallets a round takes, and each wallet costs
three purrfolio requests through a proxy plus a Grist write. Without point 4 the whole round
is one unmarked stretch, so a round longer than `HEARTBEAT_MAX_AGE` gets a healthy service
restarted mid-work — and then again on the next round, forever.

`tests/test_checker.py` drives the real `run()` with everything around it replaced and pins
each of these marks by position; every one of them was verified to make that suite red when
removed.

Two operational numbers this is built against:

- our Portainer build waits for `healthy` within `max(120s, start_period + 15s)` after
  recreating a container on the update label, and rolls the image back otherwise — hence
  `--start-period=120s` and the very first mark before any network call;
- auto-heal restarts whatever docker calls `unhealthy`, so the probe must answer "healthy"
  in every NORMAL phase of the service, including a long pause.

Note what the heartbeat does NOT mean: it says the loop is turning, not that it is
succeeding. A loop failing every Grist fetch and sleeping 10 s still marks itself alive,
which is correct — that is a service problem, not a hung process, and restarting it would
not help.

Before this refactor the image had no `HEALTHCHECK` at all, so the container's
`io.portainer.autoheal.enable` label had nothing to act on.

## The CMD is direct, on purpose

`CMD ["python", "main.py"]`. It used to be
`CMD while true; do python airdrop_checker.py; sleep 10; done`, and that wrapper made every
startup failure invisible: a container with broken configuration looked perfectly alive and
restarted itself forever, with nothing above the shell ever seeing a non-zero exit.
Restarting is `restart: unless-stopped`'s job in the stack. **Do not reintroduce a shell
loop in the CMD** — `ci/smoke.py` fails the build if PID 1 is not a python interpreter
running `main.py`.

## Behaviour that was changed on purpose

Everything here is a deliberate departure from the old single-file `airdrop_checker.py`, and
each one is observable in production. Listed so a future reader finds them as decisions
rather than as surprises:

- **A 30-second default timeout on every outgoing `requests` call**
  (`src/http_timeout.py`, installed at the top of `run()`). Carried over from
  debank_checker. `grist_api` sends its requests without any timeout at all, so a stalled
  TCP connection to Grist hangs this process forever — and there is no watchdog here to
  notice. Explicit timeouts win: the `timeout=10` on each purrfolio call is preserved,
  because the patch only fills the value in when the caller left it unset.
- **The logger is named `airdrop_checker`, not `Token checker`.** The old name was
  copy-paste from a sibling service and named the wrong program, but it is visible in every
  production log line, so anything grepping `docker logs` for it needs to be updated.
- **Proxy credentials are stripped from everything that leaves the process**
  (`redact_credentials` in `src/balances.py`). The `Proxy` setting carries
  `user:password@` and two library paths put that whole string into the text of the
  exception they raise — an unknown SOCKS scheme (`ValueError: Unable to determine SOCKS
  version from socks9://user:pass@…`, urllib3/contrib/socks.py) and a proxy URL that does
  not parse (`Failed to parse: http://user:pass@…`), both reproduced against the pinned
  requests 2.32.3 / urllib3 2.2.3. That text was logged, and `src/checker.py` writes the
  caught exception into the wallet's `Comment` column — so the password reached `docker
  logs` AND a Grist document that people open and that goes into backups. The host and port
  survive the redaction; they are what makes a proxy failure diagnosable. Every place an
  exception's text leaves the process runs through it, including `traceback.format_exc()`.
  Three things about it are worth knowing before touching it:
  - it is **best effort and not hermetic** — a regex over free-form exception text, not a
    URL parser, and the strings it must cope with are the ones that FAILED to parse as URLs.
    The password half deliberately admits `/`, spaces and `@` (an operator pastes whatever
    the vendor issued into a Grist cell, and a `/` in the password is the most common reason
    that cell produces `Failed to parse:` at all), and the match is anchored on the LAST `@`
    of the run. The price is possible over-redaction, which is the direction to err in;
  - `check_balance` re-raises with **`from None`, not `from e`**. Redacting only the new
    message left the original attached as `__cause__` with the full proxy string inside, and
    every renderer of a chain prints that second half: `traceback.format_exc()`,
    `exc_info=True`, and the interpreter's own dump of an escaping exception — which lands in
    `docker logs`. A test on `str(exception)` is green against all of that by construction,
    which is why it survived two reviews; `describe_error` is what keeps the message
    diagnosable without the chain;
  - reasons are built by **`describe_error`**, i.e. `ClassName: redacted text`. A good share
    of what this loop catches stringifies to nothing (`ConnectionError()`) or almost nothing
    (`KeyError('price')`), and the loop writes that text into the wallet's `Comment` column,
    where a bare `Error: ` is indistinguishable weeks later from a redaction that ate
    everything. A class name is not a secret.

## Non-root

The image installs `gosu`, creates `app` with a fixed uid 1000, and runs
`ENTRYPOINT ["/entrypoint.sh"]`. There is deliberately **no `USER` directive**: the
entrypoint starts as root, fixes ownership of `/app/data` (self-healing migration from
older root-based images) and drops privileges via gosu, while still respecting a compose
`user:` override.

`ci/smoke.py` verifies the drop by reading `/proc/1/status`, not `docker exec ... id -u` —
exec does not go through the ENTRYPOINT and would report 0 on a perfectly healthy container.

The HEALTHCHECK probe drops privileges **itself**, in `src/healthcheck.py`. Docker runs a
healthcheck outside the ENTRYPOINT, so the gosu chain never applies to it and — with no
`USER` directive — it would start as root every 60 seconds while the service it grades runs
as `app`. It makes the same fork the entrypoint makes: become `app` when it is actually
root, run as it is otherwise (a compose `user:` override has nothing to drop, and trying
would fail for want of permission and leave the container permanently `unhealthy`). If the
switch fails for any other reason it says so on stderr and grades the heartbeat anyway —
a probe that exited there would report "unhealthy" for a reason that has nothing to do with
the heartbeat, and hand auto-heal a restart loop.

The four privilege primitives are called through one-line wrappers on the module
(`_geteuid`, `_setgroups`, `_setgid`, `_setuid`, `_getpwnam`). That is for the tests:
`src.healthcheck.os` is the stdlib module object itself, so patching `geteuid` through it
replaces it for the entire pytest process. It also means the unit tests can only pin the
shape of the drop — the real switch is executed and observed only by `ci/smoke.py`.

## Tests

```
make install     # .venv + requirements-dev.txt (sentinel-guarded)
make test        # pytest
make run         # the loop, needs a filled .env
make clean
```

Everything is offline: HTTP is mocked at the `requests` boundary, `GristDocAPI` is replaced
by a recording double, `time.sleep` is replaced where the loop sleeps. `tests/conftest.py`
seeds the three required variables before anything can import `src.settings`, and carries an
autouse fixture that fails any test which leaves `src.http_timeout`'s module state altered —
that module holds the only mutable module-level state in the package, and a second wrapper
stacked on `Session.request` would quietly double every timeout in the process.

Tests that matter most, in the sense that they pin a decision rather than an implementation:

- each required variable missing → `SystemExit(1)` whose message names it;
- the heartbeat probe in all three states, driven with `HEARTBEAT_MAX_AGE=60` — explicitly
  NOT the 1200 s default, because a probe that ignored the variable and hardcoded 1200 would
  answer all three states correctly and pass;
- `sleep_with_heartbeat` marking several times across a long pause;
- **`run()` itself**, with the Grist client, the wallet selection, the balance lookup and
  both sleep functions replaced and the loop ended by raising a `BaseException` from a stub
  on the Nth turn (a plain exception would be swallowed by the loop's own handlers). It pins
  where every mark is written and that no branch — both `continue`s included — sleeps
  through a bare `time.sleep`;
- the probe's privilege drop in both branches, and that `main()` performs it before it
  reads the heartbeat. These patch the module's own `_geteuid`/`_setuid`/… wrappers, not
  `src.healthcheck.os` — that name IS the stdlib module, so patching through it would
  replace the primitives for the whole pytest process. They pin the SHAPE of the drop and
  nothing more: no test performs a real uid switch (locally the probe is not root; in CI
  it is root in an image with no `app` account), so the only place the switch is really
  executed AND observed is the smoke gate, group (l);
- `redact_credentials`, and a proxy failure travelling through `check_balance` without the
  password appearing in the log, in `str(exception)` or in what the loop writes to Grist —
  plus the passwords that hold a `/`, a space or an `@`, which the earlier pattern did not
  match at all, and the FULL rendered chain (`traceback.format_exception`), which is the
  half `str(exception)` cannot see;
- every redaction site in `src/checker.py`, through a recording logger. Coverage does not
  help here: those lines are executed by the `run()` tests either way, and removing the
  redaction from any of them left the suite green until a test read what they produced;
- `src.healthcheck` not importing `src.settings` (checked over the import graph).

## CI

Two workflows in `.gitea/workflows/`:

- `tests.yml` — the PR gate: `test` → `build-and-smoke`. Never logs in, never pushes, and
  contains no secret expression at all.
- `image-check-publish.yml` — push to `main`: `test` → `build` → smoke gate → login → push
  `:<sha>` then `:latest`.

Rules the two files are held to, all of them load-bearing:

- the suite runs **inside `python:3.9-slim`** — the same interpreter the image ships — with
  the work tree streamed in as a tar over stdin. No `actions/setup-python` (an unverified
  setup action that silently does nothing produces a green job that tested nothing) and no
  bind mount (the job runs inside act_runner's container while docker talks to the host's
  daemon, so `-v "$PWD:/src"` means something else there, or nothing).
  This is why `requirements-dev.txt` pins **pytest 8.4.x** and not the 9.x used elsewhere in
  the fleet: pytest 9 requires python ≥ 3.10 and cannot run on 3.9 at all.
- the smoke gate sits **between build and push**, and `docker login` comes **after** it — the
  registry credential is not in the job's docker config while arbitrary image code runs;
- no published ports and nothing polled over `127.0.0.1`: everything inside a container is
  reached with `docker exec`;
- no `${{ ... }}` inside a `run:` body — values arrive through `env:`;
- every CI container is named, and every removal carries `-v`;
- the step bodies that must agree between the two files (test, gate, container cleanups) are
  **byte-identical**. Change one, change the other, and compare them mechanically —
  `sha256` of the `run:` bodies — rather than by eye.

`ci/smoke.py` is fed to a live container over stdin and grades the assembled image: every
shipped file readable, compiling under python 3.9 and importable; every third-party import
resolving one at a time by name; PID 1 being `python main.py` and running as uid 1000; the
loop writing its heartbeat AND still moving it; `tests/`, `ci/`, `.env` and `.venv` absent
from the image; a stripped environment producing a non-zero exit whose text names the
missing variables; the probe answering correctly in all three heartbeat states AND doing so
unprivileged; and `/app/src` holding exactly the modules the gate declares. Three of those
are worth spelling out, because each was a check that could not fail — or, in the last
case, one that could not pass:

- the heartbeat is sampled **twice**, and the mark has to MOVE between the samples. One
  sample can only say how old the mark is, and moments after startup that is "seconds"
  whatever happens next — so a loop that wrote its mark once and wedged passed the old
  single-sample check by construction;
- `/app/src` is listed and compared against `SHIPPED_FILES` in BOTH directions. The image
  gets its code through `COPY src/ src/`, which ships whatever is in the directory, while
  that tuple is written by hand — so before this check a new module arrived in production
  without ever being compiled or imported by the gate.
- the probe's scratch heartbeat and its directory get **explicit modes** (0755/0644), and
  group **(l)** turns the same rule around. The gate arrives through `docker exec` as root,
  `mkdtemp` creates 0700, and the probe drops to uid 1000 before reading anything — so the
  whole of group (j) used to answer "heartbeat file missing" for a permission reason: the
  two unhealthy states passed for the wrong reason and the healthy one could not pass at
  all, which meant `smoke FAILED` and no push, ever. Group (l) then feeds the probe a fresh
  heartbeat in a root-only directory and requires it NOT to see it: only a process that
  really stopped being root can fail that read, and nothing else in the pipeline can
  observe the probe's own privilege drop.

It never uses
`assert` (asserts vanish under `PYTHONOPTIMIZE=1`, which would make the gate permanently
green), it checks every target before reporting so one run shows the full breakage, and it
**counts its own verdicts** against a declared `EXPECTED_TARGETS`. If that count ever
disagrees, do NOT edit the number to match the run — find the check that went missing.

Docker's own health verdict is waited for by the workflow rather than by the script, because
it is not visible from inside the container. An EMPTY health status is treated as a failure
of its own: it means the `HEALTHCHECK` line was removed from the Dockerfile.

## Dependencies

`requirements.txt` is a complete pinned closure, `==` throughout, transitive names included.
Two entries whose reason is not visible from the code:

- **`PySocks`** — nothing imports it by name. It is reached through `requests`, and without
  it a `socks5://` proxy string typed into the Grist `Proxy` setting fails every request
  with "Missing dependencies for SOCKS support".
- the **pydantic stack** (`pydantic`, `pydantic-core`, `annotated-types`,
  `typing-extensions`, `typing-inspection`, `python-dotenv`) is pinned alongside
  `pydantic-settings` so two builds of one commit cannot produce different images. The
  versions are ones with prebuilt cp39 manylinux wheels, so the build never compiles
  pydantic-core's Rust extension.

`jsonpath-ng` and `ply` were removed: the only code importing them (`get_value_by_jsonpath`,
`parse_and_sum_jsonpaths`) was never called from anywhere.

The base image stays `python:3.9`. It is end-of-life and moving off it is worth doing, but
that is a separate change with its own testing.

## Conventions

- Comments, log lines and error messages in **English**.
- A comment says **why**, not what. If it restates the code, delete it.
- **No real secrets in the repository, in any form** — `.env.example` holds placeholders
  only, `.env` is gitignored and kept out of the image, and CI passes obviously-fake values.
- Prefer a `make` target over a one-off shell command.
- The service's behaviour in production is a contract. Preserve it, or say out loud that you
  are changing it.
