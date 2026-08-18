#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Docker HEALTHCHECK probe for airdrop_checker.

Exit 0 (healthy) when the heartbeat file written by the main loop is fresh;
exit 1 (unhealthy) when it is missing or stale. A stale heartbeat means the
main loop is hung; combined with the container's `io.portainer.autoheal.enable`
label this triggers an automatic restart.

Run as `python -m src.healthcheck` (WORKDIR /app in the image), which is exactly
what the Dockerfile's HEALTHCHECK line does.

Configuration is read straight from the environment rather than through
`src.settings`, and that is deliberate: `Settings` requires the three Grist
variables and exits(1) when one is missing, so a probe built on it would report
"unhealthy" for a configuration reason and hide the heartbeat verdict it exists
to give — handing autoheal a container it restarts forever over a typo in the
stack's `environment:` block. The two defaults it does need are shared with
`src/settings.py` via `src/heartbeat.py`, so there is still only one definition
of each.
"""
import os
import pwd
import sys

from src.heartbeat import (
    DEFAULT_HEARTBEAT_FILE,
    DEFAULT_HEARTBEAT_MAX_AGE,
    heartbeat_age,
)

HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", DEFAULT_HEARTBEAT_FILE)
HEARTBEAT_MAX_AGE = int(os.getenv("HEARTBEAT_MAX_AGE", str(DEFAULT_HEARTBEAT_MAX_AGE)))

# The unprivileged account the image creates (`useradd -m -u 1000 app`) and the
# one entrypoint.sh hands the main loop to. Looked up by NAME rather than pinned
# to uid 1000 here, because the name is what the Dockerfile and the entrypoint
# both spell — the uid is the image's business.
APP_USER = "app"


# The five privilege primitives, wrapped one call thick, and the wrappers exist for
# the TESTS rather than for this module.
#
# `src.healthcheck.os` is not a copy of anything: it IS the stdlib `os` module
# object, so a test that reaches through it to replace `geteuid`/`setuid` replaces
# them FOR THE WHOLE PROCESS for the duration of that test — every fixture, plugin
# and helper that runs in that window is told it is root, and anything that acts on
# the answer does so in the interpreter running the suite. Patching these instead
# keeps the substitution inside this module, where it belongs.
#
# Be honest about what that buys: the tests can then pin the SHAPE of the drop (that
# it happens at all, in which order, and that a failure to drop is not fatal) and
# nothing more. No test performs a real uid switch — locally the probe returns early
# because it is not root, and in CI it runs as root inside `python:3.9-slim`, where
# there is no `app` account and the non-fatal branch fires. The only place the real
# switch is executed is `ci/smoke.py`, against a live container of the built image;
# that is also the only place that can observe it, which is what group (l) of the
# gate is for.
def _geteuid():
    return os.geteuid()


def _setgroups(groups):
    os.setgroups(groups)


def _setgid(gid):
    os.setgid(gid)


def _setuid(uid):
    os.setuid(uid)


def _getpwnam(name):
    return pwd.getpwnam(name)


def drop_privileges():
    """Run the probe as the user the service itself runs as, when it can.

    Docker runs HEALTHCHECK outside the ENTRYPOINT, so the gosu drop in
    entrypoint.sh never applies to it: with no `USER` directive in the image (and
    there is deliberately none — see the Dockerfile) this probe would start as
    root every 60 seconds while the loop it grades runs as `app`. Nothing in the
    probe NEEDS root — it reads one file's mtime — so running as root is both a
    hole in the image's claim to be unprivileged and a lost signal: the probe
    would be exercised in conditions it never sees in production.

    Two branches, the same fork entrypoint.sh makes, and the second one is what
    keeps this from becoming a new way not to start:

    * root -> become `app`, exactly what the service does with itself;
    * anything else -> proceed as is. That is a compose `user:` override (or a
      local run), where there is no privilege to drop; attempting the switch
      would fail for want of permission and turn a healthy container into a
      permanently `unhealthy` one that auto-heal restarts forever.
    """
    if _geteuid() != 0:
        return
    try:
        account = _getpwnam(APP_USER)
        # Groups first, uid LAST: after setuid the process no longer has the
        # privilege to change its groups, so the reverse order would leave root's
        # supplementary groups attached to a nominally unprivileged process.
        _setgroups([])
        _setgid(account.pw_gid)
        _setuid(account.pw_uid)
    except (KeyError, OSError) as error:
        # Deliberately NOT fatal. Exiting here would report "unhealthy" for a
        # reason that has nothing to do with the heartbeat — an image without the
        # `app` account would be restarted by auto-heal forever while its loop
        # ran perfectly. Say it on stderr (docker keeps the probe's output in the
        # health log) and grade the heartbeat anyway: less privilege hygiene for
        # this one process, but a truthful verdict.
        print("cannot drop privileges to {}: {}; probing as uid {}".format(
            APP_USER, error, _geteuid()), file=sys.stderr)


def main():
    drop_privileges()
    try:
        age = heartbeat_age(HEARTBEAT_FILE)
    except OSError:
        # Missing/unreadable heartbeat (e.g. very early startup) -> unhealthy.
        print("heartbeat file {} missing".format(HEARTBEAT_FILE), file=sys.stderr)
        return 1
    if age > HEARTBEAT_MAX_AGE:
        print("heartbeat stale: {}s > {}s".format(int(age), HEARTBEAT_MAX_AGE),
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
