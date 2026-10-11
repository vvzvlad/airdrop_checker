"""Suite-wide setup: the required environment.

Everything in this suite runs OFFLINE. Every HTTP call is mocked at the
`requests` boundary and the Grist client is replaced by a recording double, so
nothing here opens a socket.
"""

import os

# Provide the three required variables BEFORE any test module imports
# src.settings (`settings` is built at import time and would otherwise exit(1)).
# In CI the same variables are injected via the workflow's `env:` block, so the
# suite does not silently depend on this file keeping its defaults.
#
# These are obviously-fake placeholders and nothing in the suite ever performs a
# real request with them.
os.environ.setdefault("GRIST_SERVER", "http://grist.invalid")
os.environ.setdefault("GRIST_DOC_ID", "test-doc")
os.environ.setdefault("GRIST_API_KEY", "test-key")
