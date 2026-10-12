#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Entry point for airdrop_checker — a thin wrapper over `src/`.

Importing src.checker pulls in src.settings, so a missing/invalid environment
variable exits here with a message naming it (see src/config_errors.py) before
any work starts. The container is expected to die in that case; production's
`restart: unless-stopped` restarts it.
"""

from src.checker import run

if __name__ == "__main__":
    run()
