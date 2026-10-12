"""The heartbeat file: written by the main loop, read by the Docker HEALTHCHECK probe.

The single definition of the heartbeat contract. Stdlib-only on purpose: the
probe (`src/healthcheck.py`) imports it, and reaching `src.settings` would make
the probe exit on a missing Grist variable, reporting a configuration problem
instead of the heartbeat verdict.
"""

import os
import time

# Defaults for HEARTBEAT_FILE / HEARTBEAT_MAX_AGE: a production contract with the
# autoheal-enabled `airdropchecker` container in the `crypt-common` stack on nebula.
# The loop refreshes the mark every 30 s while it sleeps (see src/checker.py), so
# 1200 s does not need to cover the between-rounds pause (Grist `Settings`, minutes).
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
