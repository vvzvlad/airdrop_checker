"""The global default timeout, exercised through a real `requests.Session`.

grist_api 0.1.0 calls `requests.request(...)` with no timeout, and this service
has no watchdog of its own — so a stalled Grist connection would hang the loop
forever, stop the heartbeat and leave auto-heal as the only thing that notices.

A mounted adapter is what makes this testable offline: it records the timeout
`Session.request` passed down and answers without touching a socket.
"""

import requests

from src.http_timeout import DEFAULT_REQUEST_TIMEOUT


class _RecordingAdapter(requests.adapters.BaseAdapter):
    def __init__(self):
        super().__init__()
        self.timeouts = []

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        self.timeouts.append(timeout)
        response = requests.Response()
        response.status_code = 200
        response._content = b"{}"
        response.url = request.url
        response.request = request
        return response

    def close(self):
        pass


def _session_with_adapter():
    session = requests.Session()
    adapter = _RecordingAdapter()
    session.mount("http://", adapter)
    return session, adapter


def test_a_call_without_a_timeout_gets_the_default():
    # This is the grist_api case: the library never passes one.
    session, adapter = _session_with_adapter()
    session.get("http://grist.invalid/api/docs")
    assert adapter.timeouts == [DEFAULT_REQUEST_TIMEOUT]


def test_an_explicit_timeout_is_left_alone():
    # src/balances.py passes timeout=10 to each purrfolio call, and the patch must
    # not widen it to 30 behind its back.
    session, adapter = _session_with_adapter()
    session.get("http://grist.invalid/api/docs", timeout=10)
    assert adapter.timeouts == [10]
