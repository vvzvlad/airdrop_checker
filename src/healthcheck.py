#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Docker HEALTHCHECK probe for airdrop_checker.

Exit 0 (healthy) when the heartbeat file written by the main loop is fresh;
exit 1 (unhealthy) when it is missing or stale. A stale heartbeat means the
main loop is hung; combined with the container's `io.portainer.autoheal.enable`
label this triggers an automatic restart.

Run as `python -m src.healthcheck` (WORKDIR /app in the image), which is exactly
what the Dockerfile's HEALTHCHECK line does.

Configuration is read straight from the environment, not through
`src.settings`: `Settings` exits when a Grist variable is missing, which would
make the probe report a configuration problem instead of the heartbeat verdict.
The defaults are shared with `src/settings.py` via `src/heartbeat.py`.
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
# one entrypoint.sh hands the main loop to, looked up by name like the entrypoint.
APP_USER = "app"


# The five privilege primitives, wrapped one call thick so tests can patch them
# here instead of on the stdlib `os` module, which would affect the whole process.
# Tests pin only the shape of the drop (order, non-fatal failure); the real uid
# switch is exercised only by `ci/smoke.py` against a live container.
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

    Docker runs HEALTHCHECK outside the ENTRYPOINT and the image has no `USER`
    directive, so without this the probe would run as root while the loop runs as
    `app`. The probe only reads one file's mtime.

    Two branches, the same fork entrypoint.sh makes:

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
        # Not fatal: exiting would report "unhealthy" for a non-heartbeat reason
        # and auto-heal would restart a working container forever. Report on
        # stderr (kept in docker's health log) and grade the heartbeat anyway.
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
