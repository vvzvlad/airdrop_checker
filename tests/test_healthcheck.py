"""The Docker HEALTHCHECK probe, driven exactly the way docker drives it.

The probe is what auto-heal restarts the production container on, so these run the
real `python -m src.healthcheck` in a subprocess and look at its EXIT STATUS — not
at an imported function, which would not prove the module is runnable that way at
all.

The window is passed explicitly and is deliberately NOT the probe's own 1200 s
default: with the two equal, a probe that ignored HEARTBEAT_MAX_AGE and hardcoded
1200 would answer all three states correctly and pass every one of these.
"""

import ast
import importlib
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import src.healthcheck
from src.heartbeat import (
    DEFAULT_HEARTBEAT_FILE,
    DEFAULT_HEARTBEAT_MAX_AGE,
    heartbeat_age,
    write_heartbeat,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

# Same value ci/smoke.py uses, and for the same reason (see the module docstring).
MAX_AGE = 60
STALE_AGE = 3 * MAX_AGE


def run_probe(heartbeat_path, max_age=MAX_AGE):
    """Run the probe the way the Dockerfile's HEALTHCHECK does; return (rc, output)."""
    env = dict(os.environ)
    env["HEARTBEAT_FILE"] = str(heartbeat_path)
    env["HEARTBEAT_MAX_AGE"] = str(max_age)
    completed = subprocess.run(
        [sys.executable, "-m", "src.healthcheck"],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60,
    )
    return completed.returncode, completed.stdout.decode("utf-8", "replace")


def test_probe_reports_unhealthy_when_the_heartbeat_is_absent(tmp_path):
    status, output = run_probe(tmp_path / "absent")
    assert status != 0
    assert "missing" in output


def test_probe_reports_healthy_for_a_fresh_heartbeat(tmp_path):
    path = tmp_path / "heartbeat"
    path.write_text("now")
    status, output = run_probe(path)
    assert status == 0, output


def test_probe_reports_unhealthy_for_a_stale_heartbeat(tmp_path):
    # The SAME file as the fresh case, with only its mtime moved back: that is
    # what makes this a test of the staleness comparison rather than of the
    # existence check.
    path = tmp_path / "heartbeat"
    path.write_text("old")
    backdated = time.time() - STALE_AGE
    os.utime(path, (backdated, backdated))
    status, output = run_probe(path)
    assert status != 0
    assert "stale" in output


def test_probe_honours_heartbeat_max_age_rather_than_its_own_default(tmp_path):
    # The same backdated file is stale under a 60 s window and fresh under the
    # 1200 s default. A probe that ignored the variable would pass the three cases
    # above and fail here.
    path = tmp_path / "heartbeat"
    path.write_text("old")
    backdated = time.time() - STALE_AGE
    os.utime(path, (backdated, backdated))
    assert run_probe(path, max_age=MAX_AGE)[0] != 0
    assert run_probe(path, max_age=DEFAULT_HEARTBEAT_MAX_AGE)[0] == 0


def test_probe_defaults_are_the_production_contract(monkeypatch):
    # No HEARTBEAT_* in the environment: the module must fall back to exactly the
    # path and window the crypt-common stack and auto-heal are built around.
    monkeypatch.delenv("HEARTBEAT_FILE", raising=False)
    monkeypatch.delenv("HEARTBEAT_MAX_AGE", raising=False)
    reloaded = importlib.reload(src.healthcheck)
    try:
        assert reloaded.HEARTBEAT_FILE == "/tmp/airdrop_checker_heartbeat"
        assert reloaded.HEARTBEAT_MAX_AGE == 1200
        assert reloaded.HEARTBEAT_FILE == DEFAULT_HEARTBEAT_FILE
        assert reloaded.HEARTBEAT_MAX_AGE == DEFAULT_HEARTBEAT_MAX_AGE
    finally:
        # Restore the module to whatever the ambient environment says, so a later
        # import in this session does not see the reloaded state.
        importlib.reload(src.healthcheck)


def test_probe_does_not_import_the_application_settings():
    # If it did, a missing credential would make it exit non-zero for a
    # CONFIGURATION reason — reporting "unhealthy" on a perfectly live loop and
    # handing auto-heal a restart loop it can never break out of.
    # Checked over the import graph, not the file text, so the module docstring is
    # free to explain the rule it is being held to.
    seen = set()
    pending = ["src.healthcheck"]
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        path = REPO_ROOT.joinpath(*name.split(".")).with_suffix(".py")
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported = [node.module or ""]
            else:
                continue
            for module in imported:
                root = module.split(".")[0]
                assert module != "src.settings", \
                    "{} imports src.settings — the probe must stay independent of it".format(name)
                assert root != "pydantic", \
                    "{} imports {} — the probe must stay stdlib-only".format(name, module)
                if root == "src":
                    pending.append(module)


# --- dropping privileges -----------------------------------------------------
#
# Docker runs HEALTHCHECK outside the ENTRYPOINT, so the gosu drop in
# entrypoint.sh never applies to the probe: with no USER directive in the image
# it starts as root every 60 seconds while the loop it grades runs as `app`.
# These pin both branches of the fork the probe makes about that — the fix must
# not become a new way for a healthy container to be scored unhealthy.
#
# WHAT THESE DO NOT TEST, stated plainly because the gap is easy to mistake for
# coverage: none of them performs a real privilege drop, and the reason is the patching
# rather than the environment. `_patch_drop` replaces all five primitives, `_geteuid`
# included — it answers 0, so the root branch runs on a suite that is not root — and the
# assertions then read the recorded CALLS. Empty a wrapper's body in src/healthcheck.py
# (`def _setuid(uid): pass`) and nothing here notices; delete the CALL and
# test_a_root_probe_becomes_the_app_user_groups_first goes red on the call list.
# (The subprocess probe further up does run drop_privileges unpatched — but it has
# nothing to do there: locally the suite is not root and the probe returns at the first
# branch, and in CI it is root inside `python:3.9-slim`, which has no `app` account, so
# `pwd.getpwnam` raises and the non-fatal branch fires.)
# What is pinned is the SHAPE of the drop — that it happens, in the right order, and
# that a failure to perform it is not fatal. The only place the switch is really
# executed AND observed is `ci/smoke.py`, group (l), against a live container.
#
# They patch `src.healthcheck._geteuid` and friends — the module's own one-line
# wrappers — and NOT `src.healthcheck.os.geteuid`. The latter is the stdlib module
# object itself, so patching through it replaces the primitive for the whole process
# for the duration of the test: pytest's own internals, fixtures and plugins running
# in that window would be told they are root.


class _RecordingOS:
    """Stands in for the parts of `os` the drop uses, recording the order."""

    def __init__(self, euid, geteuid_after=None):
        self.euid = euid
        self.geteuid_after = geteuid_after
        self.calls = []

    def geteuid(self):
        return self.euid

    def setgroups(self, groups):
        self.calls.append(("setgroups", tuple(groups)))

    def setgid(self, gid):
        self.calls.append(("setgid", gid))

    def setuid(self, uid):
        self.calls.append(("setuid", uid))


class _Account:
    pw_uid = 1000
    pw_gid = 1000


def _patch_drop(monkeypatch, recording_os, getpwnam):
    monkeypatch.setattr(src.healthcheck, "_geteuid", recording_os.geteuid)
    monkeypatch.setattr(src.healthcheck, "_setgroups", recording_os.setgroups)
    monkeypatch.setattr(src.healthcheck, "_setgid", recording_os.setgid)
    monkeypatch.setattr(src.healthcheck, "_setuid", recording_os.setuid)
    monkeypatch.setattr(src.healthcheck, "_getpwnam", getpwnam)


def test_the_privilege_primitives_are_module_local_wrappers():
    # The property the three tests below depend on for their isolation: each
    # wrapper is a name on THIS module, so replacing it cannot reach the stdlib
    # `os` every other library in the process is using. If one of them is ever
    # inlined back into drop_privileges, the tests would go on passing while
    # patching `os` process-wide, which is the failure this pins.
    for name in ("_geteuid", "_setgroups", "_setgid", "_setuid", "_getpwnam"):
        wrapper = getattr(src.healthcheck, name)
        assert wrapper.__module__ == "src.healthcheck", \
            "{} is not this module's own wrapper: {!r}".format(name, wrapper)
    assert src.healthcheck._geteuid() == os.geteuid()


def test_a_root_probe_becomes_the_app_user_groups_first(monkeypatch):
    # The order is the point: after setuid the process can no longer change its
    # groups, so setgid afterwards would silently leave root's groups attached.
    recording = _RecordingOS(euid=0)
    _patch_drop(monkeypatch, recording, lambda name: _Account())
    src.healthcheck.drop_privileges()
    assert recording.calls == [("setgroups", ()), ("setgid", 1000), ("setuid", 1000)]


def test_a_non_root_probe_changes_nothing(monkeypatch):
    # A compose `user:` override, or a local run. There is no privilege to drop,
    # and attempting the switch would fail for want of permission — turning a
    # healthy container into a permanently unhealthy one that auto-heal restarts
    # forever. The probe only reads one file's mtime, so it works as it is.
    recording = _RecordingOS(euid=1000)
    def _must_not_be_called(name):
        raise AssertionError("a non-root probe looked up {} to switch to it".format(name))
    _patch_drop(monkeypatch, recording, _must_not_be_called)
    src.healthcheck.drop_privileges()
    assert recording.calls == []


def test_a_probe_that_cannot_drop_still_reports_on_the_heartbeat(monkeypatch, capsys):
    # An image without the `app` account is a real possibility (a different base,
    # a botched build). Exiting here would report "unhealthy" for a reason that
    # has nothing to do with the heartbeat, and auto-heal would restart a
    # perfectly live loop forever. It says so on stderr and grades anyway.
    recording = _RecordingOS(euid=0)
    def _no_such_user(name):
        raise KeyError("getpwnam(): name not found: {}".format(name))
    _patch_drop(monkeypatch, recording, _no_such_user)
    src.healthcheck.drop_privileges()
    assert recording.calls == []
    assert "cannot drop privileges" in capsys.readouterr().err


def test_the_probe_drops_before_it_looks_at_the_heartbeat(monkeypatch, tmp_path):
    # main() must call the drop, not merely define it: a probe that reads the file
    # first and drops afterwards is a probe that still did its work as root.
    order = []
    path = tmp_path / "heartbeat"
    path.write_text("now")
    monkeypatch.setattr(src.healthcheck, "drop_privileges", lambda: order.append("drop"))
    monkeypatch.setattr(src.healthcheck, "heartbeat_age",
                        lambda where: order.append("read") or 0)
    monkeypatch.setattr(src.healthcheck, "HEARTBEAT_FILE", str(path))
    assert src.healthcheck.main() == 0
    assert order == ["drop", "read"]


# --- the writer side ---------------------------------------------------------

def test_write_heartbeat_writes_a_current_unix_timestamp(tmp_path):
    path = tmp_path / "heartbeat"
    before = int(time.time())
    write_heartbeat(str(path))
    written = int(path.read_text())
    assert before <= written <= int(time.time())


def test_write_heartbeat_creates_the_file_if_it_is_not_there(tmp_path):
    path = tmp_path / "heartbeat"
    assert not path.exists()
    write_heartbeat(str(path))
    assert path.exists()


def test_write_heartbeat_never_raises_on_an_unwritable_path(tmp_path):
    # A failed heartbeat must not take the main loop down with it — the loop would
    # then stop checking wallets over a /tmp problem, which is the opposite of
    # what a liveness mark is for.
    class _Recorder:
        def __init__(self):
            self.warnings = []

        def warning(self, message):
            self.warnings.append(message)

    logger = _Recorder()
    write_heartbeat(str(tmp_path / "no-such-dir" / "heartbeat"), logger=logger)
    assert len(logger.warnings) == 1


def test_write_heartbeat_survives_a_bad_path_with_no_logger_at_all(tmp_path):
    # The logger is optional, and the failure path must not turn into an
    # AttributeError on None.
    write_heartbeat(str(tmp_path / "no-such-dir" / "heartbeat"))


def test_heartbeat_age_grows_with_the_files_mtime(tmp_path):
    path = tmp_path / "heartbeat"
    path.write_text("x")
    backdated = time.time() - 300
    os.utime(path, (backdated, backdated))
    assert 290 <= heartbeat_age(str(path)) <= 310


def test_heartbeat_age_raises_for_a_missing_file(tmp_path):
    with pytest.raises(OSError):
        heartbeat_age(str(tmp_path / "absent"))
