"""The heartbeat file: written by the main loop, read by the Docker HEALTHCHECK probe.

This module is the single place the heartbeat contract is defined, and it is
deliberately STDLIB-ONLY — it imports neither pydantic nor `src.settings`.

Why that matters: `src/healthcheck.py` (the probe docker runs, and the one
`autoheal` acts on) imports this module. If the probe reached `src.settings`
instead, building `Settings` would `exit(1)` whenever one of the three required
Grist variables was absent — so the probe would report "unhealthy" for a
CONFIGURATION reason and mask the heartbeat verdict it exists to give. Keeping
the defaults here lets `src/settings.py` and the probe share one definition
without the probe inheriting the app's configuration requirements.
"""

import os
import time

# Defaults for HEARTBEAT_FILE / HEARTBEAT_MAX_AGE. These two values are a
# production contract: the `airdropchecker` container in the `crypt-common` stack
# on nebula carries `io.portainer.autoheal.enable`, and this is the mark the
# HEALTHCHECK it acts on is built from. Do not change them without changing the
# stack that consumes them.
#
# 1200 s has to stay comfortably above the LONGEST pause the loop can take: the
# wait between rounds comes from the Grist `Settings` table in MINUTES, so an
# operator who types 15 there puts a 900 s gap between two rounds. The loop
# refreshes the mark every 30 s while it sleeps (see src/checker.py) so the
# window is not actually load-bearing for that case — but if that chunking were
# ever removed, this number is what would decide whether a healthy service gets
# restarted mid-pause.
DEFAULT_HEARTBEAT_FILE = "/tmp/airdrop_checker_heartbeat"
DEFAULT_HEARTBEAT_MAX_AGE = 1200  # seconds


def write_heartbeat(path, logger=None):
    """Best-effort liveness mark; never let heartbeat I/O break the main loop."""
    try:
        with open(path, "w") as handle:
            handle.write(str(int(time.time())))
    except Exception as error:  # noqa: BLE001 - a failed heartbeat must not raise
        if logger is not None:
            logger.warning("Failed to write heartbeat {}: {}".format(path, error))


def heartbeat_age(path):
    """Seconds since `path` was last written. Raises OSError when it is missing."""
    return time.time() - os.path.getmtime(path)
