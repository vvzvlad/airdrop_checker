"""Global default timeout for every outgoing `requests` call.

grist_api 0.1.0 calls `requests.request(...)` without a timeout (see
`grist_api.GristDocAPI.call`), so a stalled Grist TCP connection can hang the
whole process forever — and this service has no watchdog of its own, so "forever"
is exactly what it means: the loop would stop writing its heartbeat, the
HEALTHCHECK would go red and autoheal would be the only thing left to notice.
Patching `Session.request` injects a default timeout for every request that does
not set its own. The module-level `requests.get` / `requests.request` helpers
route through `Session.request`, so this also covers grist_api. Explicit timeouts
(the `timeout=10` on each purrfolio call in src/balances.py) are preserved via
`setdefault`.
"""

import requests

DEFAULT_REQUEST_TIMEOUT = 30  # seconds (connect + read ceiling)

# Guards against stacking wrappers if this is called more than once (a test
# importing two modules that both install it, for instance).
_installed = False


def install_default_timeout(timeout=DEFAULT_REQUEST_TIMEOUT):
    """Patch `requests.Session.request` to default `timeout` when unset."""
    global _installed
    if _installed:
        return
    original_request = requests.Session.request

    def _session_request_with_timeout(self, *args, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return original_request(self, *args, **kwargs)

    requests.Session.request = _session_request_with_timeout
    _installed = True
