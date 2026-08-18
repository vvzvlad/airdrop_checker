"""Suite-wide setup: the required environment, and a guard on module-level state.

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

import pytest  # noqa: E402 - must come after the environment is seeded
import requests  # noqa: E402

import src.http_timeout  # noqa: E402

# `src.http_timeout` holds the ONLY module-level mutable state in this package:
# an `_installed` flag plus the `requests.Session.request` it swaps out. It is
# installed here, once, so the whole session starts from the same state the real
# program runs in. `src.checker` installs it at the top of `run()` — not at import
# time, deliberately — so the tests that drive `run()` would otherwise flip that
# state somewhere in the middle of the session and make the baseline depend on
# collection order.
src.http_timeout.install_default_timeout()

_BASELINE_INSTALLED = src.http_timeout._installed
_BASELINE_SESSION_REQUEST = requests.Session.request


@pytest.fixture(autouse=True)
def module_state_is_pristine():
    """Fail the test that leaves `src.http_timeout` altered — before AND after.

    Checked on both sides on purpose. The post-condition names the test that did
    the damage; the pre-condition is what keeps the NEXT test from being blamed
    for it, which is how a state leak normally presents itself: a failure in a
    test that is perfectly correct and only fails when run after another one.
    Stacking a second wrapper on `Session.request` is the concrete accident here
    — it would double every timeout in the process quietly.
    """
    reason = "src.http_timeout state leaked {}: _installed={!r}, Session.request patched={}"
    assert src.http_timeout._installed is _BASELINE_INSTALLED and \
        requests.Session.request is _BASELINE_SESSION_REQUEST, \
        reason.format("INTO this test (an earlier test did not restore it)",
                      src.http_timeout._installed,
                      requests.Session.request is not _BASELINE_SESSION_REQUEST)
    yield
    assert src.http_timeout._installed is _BASELINE_INSTALLED and \
        requests.Session.request is _BASELINE_SESSION_REQUEST, \
        reason.format("OUT of this test", src.http_timeout._installed,
                      requests.Session.request is not _BASELINE_SESSION_REQUEST)
