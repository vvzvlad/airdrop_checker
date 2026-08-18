#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Entry point for airdrop_checker — a thin wrapper over `src/`.

Importing src.checker pulls in src.settings, so a missing/invalid environment
variable exits here with a message naming it (see src/config_errors.py) before
any work starts. The container is expected to DIE in that case: production runs
it with `restart: unless-stopped`, which is what does the restarting. That is
also why the image's CMD is a direct `python main.py` and no longer a
`while true; do ...; sleep 10; done` wrapper — the wrapper made a container with
broken configuration look perfectly alive.
"""

from src.checker import run

if __name__ == "__main__":
    run()
