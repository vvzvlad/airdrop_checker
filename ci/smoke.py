"""Smoke test for the airdrop_checker image, run against a live container.

Fed over stdin (`docker exec -i <name> python -u - < ci/smoke.py`), so it is not copied
into the image and can be run by hand the same way against a local container. Nothing
publishes a port or talks to 127.0.0.1: the CI job runs inside act_runner's container
while docker runs on the host. Pure stdlib; the only other imports are the ones it gates.

The pytest suite (run before this) covers the logic; this covers what only an assembled
image can answer:

* every shipped python file compiles under the image's python:3.9;
* every third-party module those files import resolves, one module per row;
* every first-party module imports (catches a missing COPY or `__init__.py`);
* PID 1 is `python main.py` — the ENTRYPOINT -> gosu -> CMD chain end to end;
* PID 1 runs as uid 1000, read from /proc/1/status (`docker exec id -u` bypasses the
  ENTRYPOINT and would report root on a healthy container);
* the loop writes its heartbeat and keeps moving it (two samples: one sample cannot tell
  a turning loop from one that wrote once and wedged);
* /app/src holds exactly the declared modules (`COPY src/ src/` ships the directory, the
  lists below are hand-written);
* .dockerignore kept tests/, ci/, .env and .venv out (src/settings.py would read a
  shipped .env);
* `python main.py` without the three required variables exits non-zero, names each of
  them and prints no traceback;
* healthcheck.py gives the documented verdict for an absent, fresh and stale heartbeat,
  driven through HEARTBEAT_FILE/HEARTBEAT_MAX_AGE against a scratch file;
* the probe really drops to the unprivileged user (it must fail to read a fresh heartbeat
  in a root-only directory).

Docker's own health verdict is not visible from inside; the workflow waits for it before
running this script. The container runs with placeholder configuration (see the
workflow), so no credential is used and neither Grist nor purrfolio.com is contacted.

Three properties are easy to lose:

* Failures leave through SystemExit, never `assert` (asserts vanish under
  PYTHONOPTIMIZE=1).
* Every target is checked before anything is reported, and a check that cannot run
  reports itself as FAILED rather than being skipped.
* The gate counts its own verdicts against EXPECTED_TARGETS, so a probe that quietly
  stops probing fails the run.
"""

import ast
import importlib
import os
import py_compile
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time
import traceback

# WORKDIR in the Dockerfile. Hardcoded so that moving WORKDIR fails the gate.
APP_DIR = "/app"

# Every python file the image is expected to ship, as a path relative to /app. Listed
# rather than globbed: a file that stops being COPYed has to FAIL here, and a glob would
# simply find one file fewer and report nothing at all. Keep in step with the COPY lines
# in the Dockerfile.
ENTRY_SCRIPT = "main.py"
SHIPPED_FILES = (
    ENTRY_SCRIPT,
    "src/__init__.py",
    "src/settings.py",
    "src/config_errors.py",
    "src/checker.py",
    "src/balances.py",
    "src/heartbeat.py",
    "src/healthcheck.py",
)

# The importable form of the same set. `main` is included on purpose: it is the module the
# CMD executes, and an import of it proves its `from src.checker import run` resolves from
# /app. `src.healthcheck` is NOT here — it is exercised as a subprocess further down,
# which is how docker runs it, and importing it would only prove less.
FIRST_PARTY_MODULES = (
    "main",
    "src.settings",
    "src.config_errors",
    "src.checker",
    "src.balances",
    "src.heartbeat",
)

# Top-level names that are this project's own packages/modules rather than distributions
# to install. Derived from SHIPPED_FILES, so a new top-level package joins this set by the
# same edit that gets it compiled above.
FIRST_PARTY_ROOTS = set(path.split("/")[0].replace(".py", "") for path in SHIPPED_FILES)

# The package directory the image receives wholesale (`COPY src/ src/`) and its declared
# contents, relative to it. The inventory check compares the two in both directions, so an
# unlisted module in src/ cannot escape compilation and import here.
SRC_PACKAGE = "src"
DECLARED_SRC_MODULES = tuple(
    path.split("/", 1)[1] for path in SHIPPED_FILES if path.startswith(SRC_PACKAGE + "/"))
# Directories inside /app/src that are not source; __pycache__ is written by the running loop.
IGNORED_SRC_DIRS = ("__pycache__",)

# The third-party top-level modules the shipped files are expected to import. Declared, and
# compared against the real import statements, so the row count is stable for the self-check
# and a new dependency shows up as a difference. `pydantic` is listed separately because
# src/config_errors.py imports it directly.
EXPECTED_THIRD_PARTY = ("colorama", "grist_client", "pydantic", "pydantic_settings", "requests")

# The module the Dockerfile's HEALTHCHECK runs, in the form it runs it: `python -m`. `-m`
# is what makes that work from WORKDIR /app — running the file by path would put src/ on
# sys.path instead of /app and its `from src.heartbeat import ...` would not resolve.
HEALTHCHECK_MODULE = "src.healthcheck"

# The three variables the crypt-common stack supplies. Stripped from the environment for
# the fail-loudly check, and named here so that check can assert the message names them.
CONFIG_VARS = ("GRIST_SERVER", "GRIST_DOC_ID", "GRIST_API_KEY")

# Paths .dockerignore is supposed to have kept out of the image. /app/data is deliberately
# NOT in this list: `.dockerignore` excludes the build context's `data/`, but the Dockerfile
# creates the directory itself so the entrypoint has something to chown.
EXCLUDED_PATHS = ("tests", "ci", ".env", ".venv")

# The kernel's process table, as seen from inside the container's own PID namespace: PID 1
# is the image's CMD and everything the CMD spawns is in here too. It is a module constant
# rather than a literal buried in the code so this script can also be exercised against a
# stub tree on a machine that has no /proc at all.
PROC_ROOT = "/proc"

# The uid the entrypoint is supposed to drop to (`useradd -m -u 1000 app` in the
# Dockerfile). Fixed on purpose so volume ownership does not drift between rebuilds.
APP_UID = 1000

# The heartbeat the RUNNING loop writes, and how long to wait for it. The default path is
# spelled out rather than imported from src.heartbeat: it is a production contract (the
# HEALTHCHECK and auto-heal are built on it), so a change to it must break this gate and be
# looked at, not be silently followed. The env override is honoured because the container
# may legitimately be started with one.
LIVE_HEARTBEAT_FILE = os.environ.get("HEARTBEAT_FILE", "/tmp/airdrop_checker_heartbeat")
# The loop writes it before its first Grist call and again at the top of every iteration,
# and a failed fetch costs it ten seconds, so ~30 s covers two full turns even on a slow
# runner.
HEARTBEAT_WAIT_ATTEMPTS = 60
HEARTBEAT_WAIT_PAUSE = 0.5
# How fresh the live heartbeat has to be to count as "being written". Generous next to the
# loop's own cadence; the point is only that it is not a corpse left from startup.
LIVE_HEARTBEAT_MAX_AGE = 120
# How long the mark is watched for movement after the first reading. In this container every
# Grist fetch fails fast (`grist.invalid` does not resolve), so the loop sleeps ten seconds
# through sleep_with_heartbeat, which re-marks after every chunk; 40 s leaves several marks of
# margin on a slow runner.
HEARTBEAT_ADVANCE_TIMEOUT = 40

# The window healthcheck.py is driven with, passed explicitly through HEARTBEAT_MAX_AGE. It
# must differ from the probe's 1200 s default: otherwise a probe that ignored the variable
# would pass all three states.
HEARTBEAT_MAX_AGE = 60
# How far the heartbeat is backdated for the stale case. Three times the window, so the
# verdict cannot hinge on a second of clock skew or on how long the probe took to start.
HEARTBEAT_STALE_AGE = 3 * HEARTBEAT_MAX_AGE
# Bound on a single probe run. healthcheck.py does one os.path.getmtime and returns, so
# anything approaching this means the interpreter itself is wedged. Three runs at this
# bound is 90 s worst case, which sits inside the smoke step's budget.
HEARTBEAT_PROBE_TIMEOUT = 30
# Name of the scratch heartbeat inside a fresh mkdtemp() directory; not the live path, so
# the running loop's mark cannot decide the probe's verdict.
HEARTBEAT_NAME = "smoke-heartbeat"

# Bound on the fail-loudly run. It exits while importing src.settings, so this only has to
# cover an interpreter start plus a handful of imports.
STARTUP_TIMEOUT = 60

# How much of a subprocess's output reaches the log when it disagrees with expectations.
EXCERPT_CHARS = 400

# "The image is broken, do not ship it."
EXIT_CHECKS_FAILED = 1
# "This script is broken": it returned a different number of verdicts than it declares, or
# it crashed. A separate code so the fault is not blamed on the image.
EXIT_SELF_CHECK = 3

# How many verdicts each probe group is required to return, checked before anything is
# reported. Every group returns one row per target on every path, including FAILED rows for
# checks that could not run, so a mismatch means a check went missing (a dropped check, an
# early return that emits nothing). When it fires, fix the probe, not the number. Counts
# are derived from the source tuples where possible.
EXPECTED_TARGETS = (
    ("(a) shipped files are readable", len(SHIPPED_FILES)),
    ("(b) shipped files compile", len(SHIPPED_FILES)),
    ("(c) third-party imports", 1 + len(EXPECTED_THIRD_PARTY)),
    ("(d) first-party imports", len(FIRST_PARTY_MODULES)),
    ("(e) PID 1 is the checker", 1),
    ("(f) PID 1 dropped privileges", 1),
    # Fresh, and still moving: only the second tells a turning loop from a hung one.
    ("(g) the loop writes its heartbeat and keeps moving it", 2),
    ("(h) .dockerignore kept the dev tree out", len(EXCLUDED_PATHS)),
    ("(i) missing configuration is fatal and loud", 1),
    ("(j) the healthcheck probe's three states", 3),
    ("(k) /app/src holds exactly the declared modules", 1),
    # The only check that observes the probe's privilege drop (see probe_privileges_rows).
    ("(l) the probe reads the heartbeat unprivileged", 1),
)


def python_version():
    """The interpreter this actually ran under — the image's, not the runner's."""
    return ".".join(str(part) for part in sys.version_info[:3])


def app_path(relative):
    """Where the image is expected to keep one of its files."""
    return os.path.join(APP_DIR, relative)


def read_source(relative):
    """Return (source_text, None), or (None, reason) when the file is not readable."""
    path = app_path(relative)
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as error:
        return None, (
            "cannot read {}: {} — either the image never received the file (a COPY naming "
            "a path that no longer exists) or it keeps it somewhere other than {}".format(
                path, error, APP_DIR))
    if not raw and not relative.endswith("__init__.py"):
        # An empty __init__.py is normal and is what makes the package a package; an empty
        # anything-else means a truncated copy.
        return None, "{} is 0 bytes".format(path)
    try:
        return raw.decode("utf-8"), None
    except UnicodeDecodeError as error:
        return None, "{} is not valid UTF-8: {}".format(path, error)


def compile_check(relative):
    """Byte-compile one file with the image's own interpreter; None when it compiles."""
    path = app_path(relative)
    try:
        # The .pyc goes to a temp directory instead of the default `__pycache__` next to
        # the source: this check leaves the image's filesystem exactly as it found it, and
        # keeps working if /app is ever mounted read-only.
        with tempfile.TemporaryDirectory() as workdir:
            py_compile.compile(
                path,
                cfile=os.path.join(workdir, "smoke.pyc"),
                # Without `doraise` py_compile prints the error and returns None, and this
                # check would pass on a script that does not parse — the exact silent-green
                # failure the whole file is built to avoid.
                doraise=True)
    except py_compile.PyCompileError as error:
        return "does not compile under python {}: {}".format(python_version(), error)
    except OSError as error:
        return "cannot be compiled, {} is not readable: {}".format(path, error)
    except BaseException as error:
        # BaseException for the same reason as in import_reason below: compiling executes
        # nothing, but keeping the two in step means neither can take the whole script
        # down and leave the step with an empty log.
        return "cannot be compiled: {}: {}".format(type(error).__name__, error)
    return None


def stdlib_names():
    """Every module name that belongs to THIS interpreter's standard library."""
    # Three sources, none sufficient alone:
    #   * sys.builtin_module_names: modules compiled into the interpreter;
    #   * sys.stdlib_module_names: authoritative, but only from Python 3.10 (the image is 3.9);
    #   * the stdlib directory and its lib-dynload: carries 3.9. pip installs into
    #     site-packages, which shows up as a single harmless entry.
    names = set(sys.builtin_module_names)
    names |= set(getattr(sys, "stdlib_module_names", ()))
    stdlib_dir = sysconfig.get_paths().get("stdlib")
    if stdlib_dir:
        for directory in (stdlib_dir, os.path.join(stdlib_dir, "lib-dynload")):
            try:
                entries = os.listdir(directory)
            except OSError:
                # lib-dynload does not exist on every build; the stdlib directory itself
                # not being listable is odd but not worth failing over, since the two
                # sources above still carry the common names.
                continue
            for entry in entries:
                # `datetime.py` -> datetime, `_json.cpython-39-x86_64-linux-gnu.so` ->
                # _json, a package directory -> its own name.
                name = entry.split(".")[0]
                if name:
                    names.add(name)
    return names


def declared_third_party_imports(sources):
    """Top-level names the shipped files import that are neither stdlib nor our own.

    `sources` maps a path from SHIPPED_FILES to its text. The union across ALL of them is
    what matters: src/checker.py imports src/balances.py at startup, so a requirement only the
    latter needs is just as fatal as one the entry script needs directly.

    Derived from the files' own syntax trees rather than from requirements.txt: the code is
    what has to keep working, and requirements.txt pins more than the code imports (PySocks
    is reached through requests and never named in an import statement).
    """
    imported = set()
    for relative, source in sources.items():
        tree = ast.parse(source, filename=app_path(relative))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    # `import a.b.c` -> `a`: the top-level name is the one that has to be
                    # installed, and importing it is what proves the distribution is there.
                    imported.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                # `from . import x` — a relative import is same-project by definition and
                # cannot be a missing requirement.
                if node.level:
                    continue
                if node.module:
                    imported.add(node.module.split(".")[0])
    known = stdlib_names() | FIRST_PARTY_ROOTS
    return sorted(name for name in imported if name not in known)


def import_reason(module):
    """Return None when the module imports cleanly inside the image, else the reason."""
    try:
        importlib.import_module(module)
    except BaseException as error:
        # Broad on purpose: a wrong-platform wheel or failing import-time code means the
        # program will not start, just like an ImportError. BaseException because
        # src/settings.py calls sys.exit(1) on missing configuration and importing `main`
        # reaches it; a SystemExit escaping here would end the script without a report
        # (and with status 0 if the module exited 0).
        return "{}: {}".format(type(error).__name__, error)
    return None


def source_rows():
    """(a) Read every shipped file. Returns (rows, sources, unreadable)."""
    rows = []
    sources = {}
    unreadable = []
    for relative in SHIPPED_FILES:
        source, reason = read_source(relative)
        rows.append(("read {}".format(app_path(relative)), reason))
        if source is None:
            unreadable.append(relative)
        else:
            sources[relative] = source
    return rows, sources, unreadable


def compile_rows():
    """(b) Byte-compile every shipped file under the image's own interpreter."""
    return [
        ("py_compile {} under python {}".format(app_path(relative), python_version()),
         compile_check(relative))
        for relative in SHIPPED_FILES
    ]


def third_party_rows(sources, unreadable):
    """(c) The declared set of third-party imports, then one import per declared module.

    Always 1 + len(EXPECTED_THIRD_PARTY) rows: the extraction can fail without taking the
    per-module imports with it, because those are driven by the DECLARATION and not by what
    the extraction managed to find.
    """
    target = "the {} shipped files import exactly the declared third-party modules: {}".format(
        len(SHIPPED_FILES), ", ".join(EXPECTED_THIRD_PARTY))
    if unreadable:
        # Never silently skipped, and never quietly narrowed to the files that happen to be
        # readable: an incomplete union would compare a smaller set against the declaration
        # while still printing a cheerful `ok` line, which is precisely the way a gate rots.
        rows = [(target, "not attempted: {} could not be read (see above)".format(
            ", ".join(app_path(name) for name in unreadable)))]
    else:
        try:
            found = declared_third_party_imports(sources)
        except SyntaxError as error:
            rows = [(target, "cannot be extracted, a shipped file does not parse: {}".format(
                error))]
        except Exception as error:
            rows = [(target, "cannot be extracted: {}: {}".format(
                type(error).__name__, error))]
        else:
            missing = [name for name in EXPECTED_THIRD_PARTY if name not in found]
            extra = [name for name in found if name not in EXPECTED_THIRD_PARTY]
            if missing or extra:
                # BOTH directions are failures. An extra module means a dependency entered
                # the code without this gate (or requirements.txt) being told; a missing one
                # means either the import went away — and the pin below is now checking a
                # package nothing uses — or the extraction quietly stopped finding it, which
                # would leave a real requirement ungated.
                rows = [(target, (
                    "the code and this gate disagree: {}{}{}. Either a dependency was added "
                    "or dropped without EXPECTED_THIRD_PARTY moving in the same commit, or "
                    "the extraction stopped seeing an import it used to see".format(
                        "declared but never imported: {}".format(", ".join(missing)) if missing else "",
                        "; " if missing and extra else "",
                        "imported but not declared: {}".format(", ".join(extra)) if extra else "")))]
            else:
                rows = [("{} -> {}".format(target, ", ".join(found)), None)]
    for module in EXPECTED_THIRD_PARTY:
        # One row per module, so the log names the missing requirement instead of making
        # somebody read a traceback to find out which import died.
        rows.append(("import {} inside the image".format(module), import_reason(module)))
    return rows


def first_party_rows():
    """(d) Import every module the image ships, from /app, the way the program does."""
    return [("import {} inside the image".format(module), import_reason(module))
            for module in FIRST_PARTY_MODULES]


def pid1_rows():
    """(e) PID 1 must BE `python main.py` — entrypoint, gosu and CMD, end to end."""
    target = "the container's PID 1 is `python {}`".format(ENTRY_SCRIPT)
    path = os.path.join(PROC_ROOT, "1", "cmdline")
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as error:
        return [(target, "cannot read {}: {}".format(path, error))]
    argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
    if not argv:
        return [(target, "{} is empty".format(path))]
    cmdline = " ".join(argv)
    # argv[0] must be an interpreter: a shell here means a restart-loop wrapper in the CMD
    # or an overridden entrypoint.
    if "python" not in os.path.basename(argv[0]):
        return [(target, (
            "PID 1 is {!r}, whose argv[0] is not a python interpreter — the CMD was "
            "overridden, or the failure-hiding `while true` wrapper is back".format(cmdline)))]
    if not any(arg.endswith(ENTRY_SCRIPT) for arg in argv[1:]):
        return [(target, "PID 1 is {!r}, which never names {}".format(cmdline, ENTRY_SCRIPT))]
    return [("{} ({!r})".format(target, cmdline), None)]


def privileges_rows():
    """(f) The entrypoint must have handed the process to the unprivileged app user.

    Read from /proc/1/status rather than `docker exec ... id -u`: exec bypasses the
    ENTRYPOINT and would report root (no USER directive) on a healthy container.
    """
    target = "PID 1 runs as uid {} (the entrypoint dropped root via gosu)".format(APP_UID)
    path = os.path.join(PROC_ROOT, "1", "status")
    try:
        with open(path, "r") as handle:
            status = handle.read()
    except OSError as error:
        return [(target, "cannot read {}: {}".format(path, error))]
    for line in status.splitlines():
        if line.startswith("Uid:"):
            fields = line.split()
            if len(fields) < 2:
                return [(target, "cannot parse {!r} from {}".format(line, path))]
            try:
                real_uid = int(fields[1])
            except ValueError:
                return [(target, "cannot parse {!r} from {}".format(line, path))]
            if real_uid != APP_UID:
                return [(target, (
                    "PID 1 runs as uid {}, not {} — the entrypoint did not drop privileges "
                    "(a USER directive, a missing gosu, or /entrypoint.sh not being the "
                    "ENTRYPOINT any more)".format(real_uid, APP_UID)))]
            return [("{} -> uid {}".format(target, real_uid), None)]
    return [(target, "no Uid: line in {}".format(path))]


def heartbeat_writer_rows():
    """(g) The heartbeat the probe reads has to be produced by the loop AND keep moving.

    Two rows, always. Right after startup a single reading is fresh whatever happens next;
    only a second, different mtime says the loop is still turning. The first reading tells
    "never started" from "started and stopped".
    """
    exists_target = "the running loop writes {} and it is fresh".format(LIVE_HEARTBEAT_FILE)
    advance_target = (
        "that same mark MOVES within {} s — the loop is turning, not stopped after one "
        "write".format(HEARTBEAT_ADVANCE_TIMEOUT))

    first = None
    for _ in range(HEARTBEAT_WAIT_ATTEMPTS):
        try:
            first = os.path.getmtime(LIVE_HEARTBEAT_FILE)
        except OSError:
            time.sleep(HEARTBEAT_WAIT_PAUSE)
            continue
        break
    if first is None:
        missing = (
            "no heartbeat appeared at {} in {:.0f} s — the loop is not iterating, so the "
            "HEALTHCHECK could never go green and auto-heal would restart this container "
            "forever".format(
                LIVE_HEARTBEAT_FILE, HEARTBEAT_WAIT_ATTEMPTS * HEARTBEAT_WAIT_PAUSE))
        # Two FAILED rows rather than one row and a silence: there is nothing to watch move,
        # and a check that cannot run is a failed check, never a missing one.
        return [(exists_target, missing),
                (advance_target, "not attempted: {}".format(missing))]

    age = time.time() - first
    if age > LIVE_HEARTBEAT_MAX_AGE:
        rows = [(exists_target, (
            "the file exists but is {} s old (limit {} s) — nothing has written it for far "
            "longer than a round takes".format(int(age), LIVE_HEARTBEAT_MAX_AGE)))]
    else:
        rows = [("{} -> {} s old".format(exists_target, int(age)), None)]

    # Watched from here on, whatever the first row said: a stale mark that starts moving
    # again is a slow start, and a fresh mark that never moves is the hang. Those are
    # different findings and each gets its own row.
    deadline = time.time() + HEARTBEAT_ADVANCE_TIMEOUT
    while time.time() < deadline:
        time.sleep(HEARTBEAT_WAIT_PAUSE)
        try:
            second = os.path.getmtime(LIVE_HEARTBEAT_FILE)
        except OSError as error:
            rows.append((advance_target, (
                "{} disappeared while it was being watched: {}".format(
                    LIVE_HEARTBEAT_FILE, error))))
            return rows
        if second != first:
            rows.append(("{} -> mtime advanced by {:.1f} s".format(
                advance_target, second - first), None))
            return rows
    rows.append((advance_target, (
        "the mark did not move in {} s: the loop wrote the file and stopped. Nothing else "
        "in this run can see that — the row above grades the mark's AGE, and a mark written "
        "at startup stays young for the whole of the probe's freshness window while the "
        "process behind it does nothing at all".format(HEARTBEAT_ADVANCE_TIMEOUT))))
    return rows


def src_inventory_rows():
    """(k) /app/src must hold exactly the modules declared in SHIPPED_FILES.

    `COPY src/ src/` ships the whole directory while the lists above are hand-written.
    Compared in both directions: an undeclared file means the gate under-checks; a missing
    declared file means a COPY or this tuple is out of date.
    """
    target = "/app/{} holds exactly the {} declared *.py modules".format(
        SRC_PACKAGE, len(DECLARED_SRC_MODULES))
    root = app_path(SRC_PACKAGE)
    found = []
    try:
        for directory, subdirectories, filenames in os.walk(root):
            # Pruned in place, which is what os.walk honours: not descending is cheaper than
            # filtering afterwards, and it keeps a stray __pycache__ deep in a subpackage
            # from being reported as an undeclared module.
            subdirectories[:] = [name for name in subdirectories
                                 if name not in IGNORED_SRC_DIRS]
            for filename in filenames:
                if filename.endswith(".py"):
                    found.append(os.path.relpath(
                        os.path.join(directory, filename), root).replace(os.sep, "/"))
    except OSError as error:
        return [(target, "cannot walk {}: {}".format(root, error))]

    found = sorted(found)
    declared = sorted(DECLARED_SRC_MODULES)
    if found == declared:
        return [("{} -> {}".format(target, ", ".join(found)), None)]
    undeclared = [name for name in found if name not in declared]
    absent = [name for name in declared if name not in found]
    return [(target, (
        "the image and this gate disagree: {}{}{}. A module that reached the image without "
        "reaching SHIPPED_FILES is one nothing above compiled or imported — it is in "
        "production, ungated. A declared module the image does not have means a COPY line "
        "stopped covering it".format(
            "in the image but not declared: {}".format(", ".join(undeclared)) if undeclared else "",
            "; " if undeclared and absent else "",
            "declared but not in the image: {}".format(", ".join(absent)) if absent else "")))]


def excluded_path_rows():
    """(h) .dockerignore kept the development tree out of the image.

    One row per path, always. `.env` is the sharpest of them: `src/settings.py` declares
    `SettingsConfigDict(env_file=".env")`, so a .env that reached /app would not merely sit
    in the image where anyone with pull access can read it — it would be READ, and the
    container would run on a developer's credentials instead of the stack's.
    """
    rows = []
    for relative in EXCLUDED_PATHS:
        path = app_path(relative)
        target = "{} is NOT in the image (.dockerignore)".format(path)
        if os.path.exists(path):
            rows.append((target, (
                "it exists inside the image. Either .dockerignore stopped excluding it or a "
                "COPY line was widened to `COPY . .` — which ships the test suite, the CI "
                "scripts and any local .env into a registry")))
        else:
            rows.append((target, None))
    return rows


def missing_configuration_rows():
    """(i) No configuration must mean a clear message and a non-zero exit, not a traceback."""
    target = (
        "`python {}` with {} stripped exits non-zero and names every missing variable"
        .format(ENTRY_SCRIPT, "/".join(CONFIG_VARS)))
    env = dict(os.environ)
    for name in CONFIG_VARS:
        env.pop(name, None)
    try:
        completed = subprocess.run(
            [sys.executable, ENTRY_SCRIPT],
            cwd=APP_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=STARTUP_TIMEOUT)
    except subprocess.TimeoutExpired:
        return [(target, (
            "it did not exit within {} s — with no configuration it must fail during "
            "startup, not hang or start working".format(STARTUP_TIMEOUT)))]
    except Exception as error:
        return [(target, "could not be run at all: {}: {}".format(
            type(error).__name__, error))]
    output = completed.stdout.decode("utf-8", "replace")
    if completed.returncode == 0:
        return [(target, (
            "it exited 0 with NO configuration at all. Either something is supplying "
            "credentials to this check, or the startup validation is gone — and a container "
            "that survives a broken config is one that looks alive while doing nothing; "
            "output: {!r}".format(output[:EXCERPT_CHARS])))]
    missing_from_message = [name for name in CONFIG_VARS if name not in output]
    if missing_from_message:
        return [(target, (
            "it exited {} but the message never names {}; the whole point of the guard is "
            "that the log says which variable is missing. Output: {!r}".format(
                completed.returncode, ", ".join(missing_from_message),
                output[:EXCERPT_CHARS])))]
    if "Traceback (most recent call last)" in output:
        return [(target, (
            "it exited {} with a raw traceback instead of the readable message. Output: "
            "{!r}".format(completed.returncode, output[:EXCERPT_CHARS])))]
    return [("{} -> exit {}".format(target, completed.returncode), None)]


def probe_heartbeat(path, max_age):
    """Run the image's OWN healthcheck module against a scratch heartbeat file.

    Returns (exit_status, combined output). Both inputs of the probe are environment
    variables, which is the whole reason this is testable without a credential: nothing
    else decides its verdict.
    """
    env = dict(os.environ)
    env["HEARTBEAT_FILE"] = path
    env["HEARTBEAT_MAX_AGE"] = str(max_age)
    completed = subprocess.run(
        # The image's interpreter, running the image's own copy of the probe, invoked
        # exactly the way the Dockerfile's HEALTHCHECK invokes it — not an import of it
        # into this process, which would let this script's environment and already
        # imported modules decide the answer.
        [sys.executable, "-m", HEALTHCHECK_MODULE],
        cwd=APP_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=HEARTBEAT_PROBE_TIMEOUT)
    return completed.returncode, completed.stdout.decode("utf-8", "replace").strip()


def heartbeat_state_row(target, path, max_age, want_healthy):
    """One state of the probe: run it, require the documented verdict, report the code."""
    try:
        status, output = probe_heartbeat(path, max_age)
    except subprocess.TimeoutExpired:
        return target, (
            "the probe did not finish within {} s — it reads one file's mtime, so this "
            "means the interpreter or the filesystem is wedged, and docker's own "
            "--timeout=10s would have scored the same run unhealthy".format(
                HEARTBEAT_PROBE_TIMEOUT))
    except Exception as error:
        return target, "the probe could not be run at all: {}: {}".format(
            type(error).__name__, error)
    # Zero vs non-zero is the entire contract, and deliberately not "exactly 1": the
    # Dockerfile runs `python -m src.healthcheck || exit 1`, which folds every non-zero
    # status into the 1 docker wants (and neutralises 2, which docker reserves). Pinning
    # the exact code here would redden on a change docker itself cannot see.
    if want_healthy and status != 0:
        return target, "exited {} (expected 0, i.e. healthy); probe output: {!r}".format(
            status, output[:EXCERPT_CHARS])
    if not want_healthy and status == 0:
        return target, (
            "exited 0, i.e. reported HEALTHY, where the documented verdict is unhealthy — "
            "with this probe green on a heartbeat that is missing or stale, a wedged main "
            "loop would never be restarted by auto-heal again; probe output: {!r}".format(
                output[:EXCERPT_CHARS]))
    return "{} -> exit {}".format(target, status), None


def make_probe_scratch_dir():
    """A scratch directory the PROBE can read, not merely one root can read.

    Returns (path, None) or (None, reason).

    This script runs as root and `mkdtemp` creates a 0700 root-owned directory, but the
    probe drops to uid 1000 at once and could not traverse it, reporting "missing" for
    every state. The directory is therefore 0755 and the file 0644 (see
    write_probe_heartbeat), the shape of the production mark. The directory mode is what
    decides the verdict (stat needs only traversal); group (l) uses that asymmetry from
    the other side.
    """
    try:
        workdir = tempfile.mkdtemp(prefix="smoke-heartbeat-")
    except OSError as error:
        return None, "cannot create a scratch directory: {}".format(error)
    try:
        os.chmod(workdir, 0o755)
    except OSError as error:
        shutil.rmtree(workdir, ignore_errors=True)
        return None, (
            "cannot make the scratch directory {} traversable ({}); the probe drops to "
            "uid {} before it reads anything, so it could not see the file at all".format(
                workdir, error, APP_UID))
    return workdir, None


def write_probe_heartbeat(path):
    """Create the scratch heartbeat readable by the uid the probe drops to."""
    try:
        with open(path, "w") as handle:
            handle.write("smoke")
        # Explicit, not umask-dependent: root's umask in this image would leave 0644
        # anyway, but "would anyway" is how this check broke the first time. The probe
        # reads this file as uid 1000.
        os.chmod(path, 0o644)
    except OSError as error:
        return "cannot create the scratch heartbeat {}: {}".format(path, error)
    return None


def heartbeat_probe_rows():
    """(j) Drive the probe through its three states; one row each, never fewer."""
    absent_target = (
        "{} reports UNHEALTHY (non-zero) when the heartbeat file does not exist".format(
            HEALTHCHECK_MODULE))
    fresh_target = (
        "{} reports HEALTHY (exit 0) for a heartbeat written just now, against "
        "HEARTBEAT_MAX_AGE={}s".format(HEALTHCHECK_MODULE, HEARTBEAT_MAX_AGE))
    stale_target = (
        "{} reports UNHEALTHY (non-zero) for that SAME file backdated {}s, well past "
        "HEARTBEAT_MAX_AGE={}s".format(
            HEALTHCHECK_MODULE, HEARTBEAT_STALE_AGE, HEARTBEAT_MAX_AGE))
    targets = (absent_target, fresh_target, stale_target)

    # A fresh empty directory, so the "absent" state below is absent by construction
    # and cannot be spoiled by anything left over from an earlier run — and one the
    # probe's own uid can traverse, which is the whole story in make_probe_scratch_dir.
    workdir, reason = make_probe_scratch_dir()
    if workdir is None:
        # Three FAILED rows rather than three missing ones: a check that cannot run is a
        # failed check.
        return [(target, "not attempted: {}".format(reason)) for target in targets]

    try:
        path = os.path.join(workdir, HEARTBEAT_NAME)
        rows = [heartbeat_state_row(absent_target, path, HEARTBEAT_MAX_AGE, False)]

        reason = write_probe_heartbeat(path)
        if reason is not None:
            rows.append((fresh_target, "not attempted: {}".format(reason)))
            rows.append((stale_target, "not attempted: {}".format(reason)))
            return rows
        rows.append(heartbeat_state_row(fresh_target, path, HEARTBEAT_MAX_AGE, True))

        # The SAME file, with only its mtime moved: that is what makes this a test of the
        # probe's staleness logic rather than of its existence check. If the two states
        # used different files, a probe that had lost its age comparison entirely would
        # still pass both.
        backdated = time.time() - HEARTBEAT_STALE_AGE
        try:
            os.utime(path, (backdated, backdated))
        except OSError as error:
            rows.append((stale_target, (
                "not attempted: cannot backdate the scratch heartbeat {}: {}".format(
                    path, error))))
            return rows
        rows.append(heartbeat_state_row(stale_target, path, HEARTBEAT_MAX_AGE, False))
        return rows
    finally:
        # Inside the container this is a temp directory in a filesystem that dies with the
        # container anyway; removed regardless so the script leaves nothing behind when it
        # is run by hand against something longer-lived.
        shutil.rmtree(workdir, ignore_errors=True)


def probe_privileges_rows():
    """(l) The probe must really BECOME the app user, not merely intend to.

    Group (f) grades PID 1. Docker starts a HEALTHCHECK outside the ENTRYPOINT, so the
    probe drops privileges itself in `src/healthcheck.py`, and the unit tests only pin the
    shape of the calls. The oracle: a fresh heartbeat inside a 0700 root-owned directory.
    Root would grade it fresh (exit 0); a process that stopped being root cannot traverse
    it and answers non-zero. Which unprivileged uid it reached is not checked here.
    """
    target = ("{} does its reading unprivileged — it cannot see a heartbeat only root "
              "can reach".format(HEALTHCHECK_MODULE))

    if os.geteuid() != 0:
        # Not a skip: from a non-root exec the two outcomes are indistinguishable (the
        # probe returns early having nothing to drop), so this check cannot be
        # performed and says so. Run the gate the way CI does — `docker exec` with no
        # --user — and it becomes decidable again.
        return [(target, (
            "not attempted: this script is running as uid {}, and a probe started by a "
            "non-root process has nothing to drop, so a working drop and a missing one "
            "look identical from here".format(os.geteuid())))]

    try:
        # mkdtemp already creates 0700, and it is set again explicitly for the same
        # reason group (j) sets 0755 explicitly: the mode is the entire experiment, so
        # it is stated rather than inherited from a library default that could change.
        workdir = tempfile.mkdtemp(prefix="smoke-rootonly-")
        os.chmod(workdir, 0o700)
    except OSError as error:
        return [(target, "not attempted: cannot create a root-only scratch directory: "
                         "{}".format(error))]

    try:
        path = os.path.join(workdir, HEARTBEAT_NAME)
        try:
            with open(path, "w") as handle:
                handle.write("smoke")
        except OSError as error:
            return [(target, "not attempted: cannot create {}: {}".format(path, error))]

        try:
            status, output = probe_heartbeat(path, HEARTBEAT_MAX_AGE)
        except subprocess.TimeoutExpired:
            return [(target, "the probe did not finish within {} s".format(
                HEARTBEAT_PROBE_TIMEOUT))]
        except Exception as error:
            return [(target, "the probe could not be run at all: {}: {}".format(
                type(error).__name__, error))]

        if status == 0:
            return [(target, (
                "it graded a heartbeat that only root can reach as fresh, so it was "
                "still root when it read it: drop_privileges did not switch uid. Either "
                "the switch was removed, or the image lost the `app` account and the "
                "probe took its non-fatal branch. Either way docker runs a root process "
                "on every HEALTHCHECK interval beside a service that goes to some trouble "
                "not to be one; probe output: {!r}".format(output[:EXCERPT_CHARS])))]
        return [("{} -> exit {}".format(target, status), None)]
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def main():
    # Each group's rows are kept in a local of their own rather than poured straight into
    # one shared list, purely so the self-check below can still tell them apart: once they
    # are concatenated there is no way to know which group contributed how many, which is
    # exactly the information needed to name the probe that went quiet.
    read_group, sources, unreadable = source_rows()
    compile_group = compile_rows()
    third_party_group = third_party_rows(sources, unreadable)
    first_party_group = first_party_rows()
    pid1_group = pid1_rows()
    privileges_group = privileges_rows()
    heartbeat_writer_group = heartbeat_writer_rows()
    excluded_group = excluded_path_rows()
    missing_config_group = missing_configuration_rows()
    probe_group = heartbeat_probe_rows()
    inventory_group = src_inventory_rows()
    probe_privileges_group = probe_privileges_rows()

    # SAME ORDER AS EXPECTED_TARGETS, and that is a requirement rather than a convention:
    # the pairing below is positional, so a group moved here without moving its declaration
    # is compared against somebody else's count. Several groups return 1 verdict each, so
    # swapping exactly those would satisfy every check below and go green while each group's
    # failures were reported under another one's name. Nothing in this file can detect that;
    # keeping the two tuples in step by eye is what prevents it, which is why the labels
    # carry letters.
    produced = (
        read_group,
        compile_group,
        third_party_group,
        first_party_group,
        pid1_group,
        privileges_group,
        heartbeat_writer_group,
        excluded_group,
        missing_config_group,
        probe_group,
        inventory_group,
        probe_privileges_group,
    )

    # Three self-checks, and they are three because each one catches a break the others
    # cannot see.
    wiring = []

    # (1) SAME NUMBER OF PROBE GROUPS. Its own check, because the per-group comparison below
    # is structurally incapable of making it: `zip` stops at the shorter of its arguments and
    # says nothing about the surplus. A refactor that drops a group from `produced` — rather
    # than leaving it in place returning [] — pairs the survivors against the first N
    # declarations, finds every one consistent, and reports nothing.
    arity_agrees = len(produced) == len(EXPECTED_TARGETS)
    if not arity_agrees:
        wiring.append(
            "this gate's own wiring is inconsistent: EXPECTED_TARGETS declares {} probe "
            "group(s) and `produced` in main() carries {}. A probe was added or removed "
            "without updating the constant in the same commit, so the surviving groups are "
            "no longer even paired with the declarations they are being compared against"
            .format(len(EXPECTED_TARGETS), len(produced)))

    # (2) PER GROUP: which specific probe returned the wrong number of verdicts. Only
    # attempted when the arity agrees — pairing two lists of different lengths positionally
    # would attribute counts to the wrong labels and send the reader after the wrong probe.
    if arity_agrees:
        miscounted = [
            (label, expected, len(actual))
            for (label, expected), actual in zip(EXPECTED_TARGETS, produced)
            if len(actual) != expected
        ]
    else:
        miscounted = []

    rows = []
    for group in produced:
        rows.extend(group)

    # (3) THE TOTAL, against the sum of the declarations. Belt and braces over (1) and (2),
    # and it earns its place on a case neither of them can see: a group poured into `rows` a
    # SECOND time — a duplicated `rows.extend(...)`, a copy-paste while adding a probe —
    # leaves the arity right and every per-group count right, because both of those inspect
    # `produced` and this mistake happens after it.
    declared_total = sum(count for _, count in EXPECTED_TARGETS)
    if len(rows) != declared_total:
        wiring.append(
            "the verdicts actually collected do not add up to what is declared: "
            "EXPECTED_TARGETS sums to {} and this run concatenated {}. If no probe is named "
            "below, the arithmetic broke between the probes and `rows` — a group extended "
            "into `rows` twice, or one left out of the loop entirely".format(
                declared_total, len(rows)))

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    # Reported first, and reported in full even when the self-check below is also going to
    # fire: "the image is broken" and "the gate lost a check" are two independent facts, and
    # a run that shows only one of them sends whoever reads it after half of the problem.
    if failures:
        print("")
        print("smoke FAILED: {}/{} targets broken".format(len(failures), len(rows)))
        for target in failures:
            print("  - {}".format(target))

    # The self-check comes BEFORE the success line, so `smoke ok` can never be printed by a
    # run that returned fewer verdicts than it promised — which is the whole scenario this
    # exists for, since a shrinking gate reports success by construction.
    if wiring or miscounted:
        print("")
        print("smoke SELF-CHECK FAILED: this gate did not return the verdicts it declares.")
        for problem in wiring:
            print("  - {}".format(problem))
        for label, expected, actual in miscounted:
            print("  - {}: declared {} verdict(s), returned {}".format(label, expected, actual))
        print("")
        print("This is a finding about THIS SCRIPT, not about the image: either a probe "
              "stopped returning one of its verdicts, or a probe was added to (or removed "
              "from) this file without EXPECTED_TARGETS being updated in the same commit. "
              "Either way a check was not performed and nothing above reports on it in "
              "either direction. Work out which check went missing — do NOT reconcile "
              "EXPECTED_TARGETS with the number this run produced, which would make the "
              "gate agree with itself forever.")
        raise SystemExit(EXIT_SELF_CHECK)

    if failures:
        raise SystemExit(EXIT_CHECKS_FAILED)

    print("")
    print("smoke ok: {}/{} targets".format(len(rows), len(rows)))


if __name__ == "__main__":
    # Verdicts leave through SystemExit, which `except Exception` does not catch. An
    # unplanned exception would otherwise exit 1, the "image is broken" code, so it is
    # mapped to the self-check code instead. stdout is flushed first so main()'s rows
    # (block-buffered under `docker exec`) precede the traceback.
    try:
        main()
    except Exception:
        sys.stdout.flush()
        traceback.print_exc()
        print(
            "\nsmoke CRASHED: the exception above came out of this script, not out of the "
            "image. Nothing here graded the artefact, so this run says nothing about whether "
            "the image is fit to publish — exiting {} (`the gate is broken`) rather than {} "
            "(`the image is broken`).".format(EXIT_SELF_CHECK, EXIT_CHECKS_FAILED),
            file=sys.stderr)
        raise SystemExit(EXIT_SELF_CHECK)
