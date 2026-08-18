"""Wallet selection and the purrfolio lookup, against mocked HTTP.

`requests.get` is replaced wholesale, so nothing here opens a socket. What is
tested is the part that decides the numbers written into the Grist document: the
two divisions by the HYPE price, and the regex that has to survive a "$1,234.56"
coming back from an endpoint that usually answers with a bare number.
"""

import re
import traceback

import pytest
import requests

import src.balances
from src.balances import check_balance, describe_error, find_none_values, generate_proxy

PRICE_URL = "https://purrfolio.com/api/hype-price"
DEBANK_URL = "https://purrfolio.com/api/debank-data?address="
HYPERCORE_URL = "https://purrfolio.com/api/hypercore-holdings?address="

ADDRESS = "0x1111111111111111111111111111111111111111"


class _NullLogger:
    def __init__(self):
        self.errors = []

    def error(self, message):
        self.errors.append(message)

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _RecordingGet:
    """Stands in for `requests.get`, answering by URL prefix and recording calls."""

    def __init__(self, price, usd_value, grand_total, fail_on=None):
        self.payloads = {
            PRICE_URL: {"price": price},
            DEBANK_URL: {"usd_value": usd_value},
            HYPERCORE_URL: {"grandTotal": grand_total},
        }
        self.fail_on = fail_on
        self.calls = []

    def __call__(self, url, proxies=None, timeout=None):
        self.calls.append({"url": url, "proxies": proxies, "timeout": timeout})
        for prefix, payload in self.payloads.items():
            if url.startswith(prefix):
                if self.fail_on == prefix:
                    raise RuntimeError("network is down")
                return _Response(payload)
        raise AssertionError("unexpected URL requested: {}".format(url))


@pytest.fixture
def logger():
    return _NullLogger()


# --- check_balance -----------------------------------------------------------

def test_both_values_are_the_usd_amounts_divided_by_the_hype_price(monkeypatch, logger):
    # The arithmetic that produces the two numbers in Grist. hypercore comes from
    # the hypercore endpoint's grandTotal, hyperevm from debank's usd_value, and
    # BOTH are divided by the same price — a swap here would be invisible in the
    # document until somebody compared it against a wallet by hand.
    get = _RecordingGet(price=50.0, usd_value=1000.0, grand_total=2500.0)
    monkeypatch.setattr(src.balances.requests, "get", get)
    hypercore, hyperevm = check_balance(ADDRESS, logger)
    assert hypercore == 2500.0 / 50.0 == 50.0
    assert hyperevm == 1000.0 / 50.0 == 20.0


def test_the_three_endpoints_are_called_in_order_with_the_address_appended(monkeypatch, logger):
    # The price has to be fetched FIRST: it is the divisor for both values, so a
    # reordering that moved it after a failing call would change which wallets
    # get written at all.
    get = _RecordingGet(price=2.0, usd_value=4.0, grand_total=6.0)
    monkeypatch.setattr(src.balances.requests, "get", get)
    check_balance(ADDRESS, logger)
    assert [call["url"] for call in get.calls] == [
        PRICE_URL,
        DEBANK_URL + ADDRESS,
        HYPERCORE_URL + ADDRESS,
    ]


def test_currency_formatting_is_stripped_before_the_numbers_are_parsed(monkeypatch, logger):
    # These endpoints answer with "$1,234.56" as readily as with 1234.56, and
    # float() on the formatted form raises a ValueError that names neither the
    # endpoint nor the wallet.
    get = _RecordingGet(price="$10.00", usd_value="$1,234.50", grand_total="2 500,00 USD")
    monkeypatch.setattr(src.balances.requests, "get", get)
    hypercore, hyperevm = check_balance(ADDRESS, logger)
    # "2 500,00 USD" keeps only digits and dots -> 250000 (the comma is a
    # thousands separator to the regex, not a decimal point). Pinned as the
    # existing behaviour, not endorsed as the right reading of that string.
    assert hypercore == 250000.0 / 10.0
    assert hyperevm == 1234.50 / 10.0


def test_the_proxy_reaches_requests_on_every_call(monkeypatch, logger):
    # The whole point of the proxy setting is that purrfolio sees the proxy's exit
    # IP and not the container's. A call that quietly went out direct would look
    # identical in the log and in Grist.
    get = _RecordingGet(price=1.0, usd_value=1.0, grand_total=1.0)
    monkeypatch.setattr(src.balances.requests, "get", get)
    check_balance(ADDRESS, logger, "http://user:pass@proxy.invalid:8080")
    assert len(get.calls) == 3
    for call in get.calls:
        assert call["proxies"] == {
            "http": "http://user:pass@proxy.invalid:8080",
            "https": "http://user:pass@proxy.invalid:8080",
        }
        assert call["timeout"] == 10


def test_without_a_proxy_requests_is_asked_for_a_direct_connection(monkeypatch, logger):
    get = _RecordingGet(price=1.0, usd_value=1.0, grand_total=1.0)
    monkeypatch.setattr(src.balances.requests, "get", get)
    check_balance(ADDRESS, logger)
    assert all(call["proxies"] is None for call in get.calls)


@pytest.mark.parametrize("failing", [PRICE_URL, DEBANK_URL, HYPERCORE_URL])
def test_any_failed_request_raises_with_the_address_in_the_message(monkeypatch, logger, failing):
    # The caller writes this message into the wallet's own row, so an error that
    # does not name the wallet cannot be attributed once a few rounds have gone by.
    get = _RecordingGet(price=1.0, usd_value=1.0, grand_total=1.0, fail_on=failing)
    monkeypatch.setattr(src.balances.requests, "get", get)
    with pytest.raises(Exception) as exc_info:
        check_balance(ADDRESS, logger)
    assert ADDRESS in str(exc_info.value)
    assert any(ADDRESS in message for message in logger.errors)


def test_a_zero_price_is_not_swallowed(monkeypatch, logger):
    # A price of 0 would make both divisions raise, and that has to travel as a
    # failure rather than as a written value.
    get = _RecordingGet(price=0, usd_value=1.0, grand_total=1.0)
    monkeypatch.setattr(src.balances.requests, "get", get)
    with pytest.raises(Exception) as exc_info:
        check_balance(ADDRESS, logger)
    assert ADDRESS in str(exc_info.value)


# --- generate_proxy ----------------------------------------------------------

def test_the_placeholder_is_replaced_by_a_token():
    result = generate_proxy("http://user-session-{random_token}:pass@proxy.invalid:8080")
    assert "{random_token}" not in result
    assert re.match(r"^http://user-session-[0-9a-f-]{10}:pass@proxy\.invalid:8080$", result)


def test_two_calls_produce_different_sessions():
    # The token pins a sticky residential session, so identical output would mean
    # every round leaves through the same exit IP.
    template = "http://user-{random_token}:pass@proxy.invalid:8080"
    assert generate_proxy(template) != generate_proxy(template)


def test_a_string_without_the_placeholder_is_returned_unchanged():
    # Rotation is simply off in that case; it is not an error.
    plain = "http://user:pass@proxy.invalid:8080"
    assert generate_proxy(plain) == plain


# --- redact_credentials ------------------------------------------------------
#
# The proxy string comes from the Grist `Proxy` setting and carries
# `user:password@` in production. Two library paths put that whole string into the
# text of the exception they raise (both reproduced against the pinned
# requests 2.32.3 / urllib3 2.2.3): an unknown SOCKS scheme —
# `ValueError: Unable to determine SOCKS version from socks9://user:pass@host` —
# and a proxy URL that does not parse — `Failed to parse: http://user:pass@[...`.
# That text is logged AND written into the wallet's Grist `Comment` column, so
# without this it ends up in `docker logs` and in a document people open.

SOCKS_VERSION_ERROR = ("Unable to determine SOCKS version from "
                       "socks9://puser:hunter2@proxy.invalid:1080")
PARSE_ERROR = "Failed to parse: http://puser:hunter2@[proxy.invalid"


@pytest.mark.parametrize("text", [SOCKS_VERSION_ERROR, PARSE_ERROR])
def test_the_password_is_gone_and_the_host_stays(text):
    # The host and port are what make a proxy failure diagnosable and are not the
    # secret; the credentials are the secret and nothing else.
    redacted = src.balances.redact_credentials(text)
    assert "hunter2" not in redacted
    assert "puser" not in redacted
    assert "proxy.invalid" in redacted


def test_an_exception_object_is_redacted_as_readily_as_a_string():
    # Every call site passes the caught exception itself, not str(e).
    redacted = src.balances.redact_credentials(ValueError(SOCKS_VERSION_ERROR))
    assert "hunter2" not in redacted
    assert "socks9://***@proxy.invalid:1080" in redacted


def test_credentials_without_a_scheme_are_redacted_too():
    # Some messages quote the proxy without its scheme; the userinfo is still the
    # secret there.
    assert "hunter2" not in src.balances.redact_credentials(
        "proxy rejected puser:hunter2@proxy.invalid:1080")


def test_every_occurrence_in_one_message_is_redacted():
    # A retry chain repeats the proxy URL several times in a single message, and
    # a single-substitution implementation would leave all but the first.
    doubled = SOCKS_VERSION_ERROR + " / retry: " + SOCKS_VERSION_ERROR
    assert src.balances.redact_credentials(doubled).count("hunter2") == 0


def test_redacting_is_idempotent():
    # It runs at several layers — check_balance redacts, then the loop redacts the
    # message again on its way into Grist — so a second pass must not mangle it.
    once = src.balances.redact_credentials(SOCKS_VERSION_ERROR)
    assert src.balances.redact_credentials(once) == once


def test_text_without_credentials_is_returned_unchanged():
    # The common case by far, and the one where a greedy pattern would eat a URL.
    plain = "HTTPSConnectionPool(host='purrfolio.com', port=443): Max retries exceeded"
    assert src.balances.redact_credentials(plain) == plain


def test_an_empty_or_missing_proxy_is_not_an_error():
    # This runs on the way OUT, on a log line or a Grist write. It must never be
    # the reason one of those fails — including when the proxy setting was empty
    # or never resolved at all.
    assert src.balances.redact_credentials("") == ""
    assert src.balances.redact_credentials(None) == ""


# The three shapes the previous, narrower pattern (`[^\s/@]*` for the password) did
# not redact at all, or redacted only halfway. None of them is exotic: the proxy
# string is typed by a person into a Grist cell, so the password is whatever the
# vendor issued — and a '/' in it is the single most common reason urllib3 answers
# `Failed to parse:` in the first place, which makes the message most likely to be
# logged the one that used to be least likely to be redacted.
AWKWARD_PASSWORDS = [
    ("Failed to parse: http://puser:hun/ter2@[proxy.invalid",
     "Failed to parse: http://***@[proxy.invalid"),
    ("Unable to determine SOCKS version from socks9://puser:hun ter2@proxy.invalid:1080",
     "Unable to determine SOCKS version from socks9://***@proxy.invalid:1080"),
    # The '@' inside the password is what the old pattern anchored on, leaving the
    # tail of the password — `sswOrd` — in the message.
    ("http://puser:p@sswOrd@proxy.invalid:8080",
     "http://***@proxy.invalid:8080"),
]


@pytest.mark.parametrize("text,expected", AWKWARD_PASSWORDS)
def test_a_password_holding_a_slash_a_space_or_an_at_is_redacted_whole(text, expected):
    redacted = src.balances.redact_credentials(text)
    assert redacted == expected
    # Spelled out as well as compared, so a failure says WHICH half survived rather
    # than only that two strings differ.
    for secret in ("hun/ter2", "hun ter2", "p@sswOrd", "sswOrd", "puser"):
        assert secret not in redacted
    assert "proxy.invalid" in redacted
    # Redaction runs at several layers (check_balance redacts, then the loop redacts
    # the result again), so a second pass must be a no-op for these too.
    assert src.balances.redact_credentials(redacted) == redacted


def test_a_url_with_a_port_and_a_later_address_is_left_alone():
    # The counterweight to the width above: the password may span at most one run of
    # whitespace. Without that bound, everything between a `scheme://token:` and any
    # later '@' on the line is eaten as a "password" and the message loses its middle
    # — which would make a redacted log harder to read than an unredacted one.
    text = "http://proxy.invalid:8080 refused the request for admin@example.com"
    assert src.balances.redact_credentials(text) == text


def test_a_proxy_failure_leaks_nothing_through_the_log_or_the_raise(monkeypatch, logger):
    # End to end at the boundary that actually leaks: the failing request carries
    # the proxy URL in its message, and check_balance both logs it and re-raises
    # it for the loop to write into Grist.
    class _LeakyGet:
        def __call__(self, url, proxies=None, timeout=None):
            raise ValueError(SOCKS_VERSION_ERROR)

    monkeypatch.setattr(src.balances.requests, "get", _LeakyGet())
    with pytest.raises(Exception) as exc_info:
        check_balance(ADDRESS, logger, "socks9://puser:hunter2@proxy.invalid:1080")
    assert "hunter2" not in str(exc_info.value)
    assert "puser:hunter2@" not in str(exc_info.value)
    assert logger.errors and all("hunter2" not in message for message in logger.errors)
    # Still attributable: the wallet and the proxy host survive the redaction.
    assert ADDRESS in str(exc_info.value)
    assert "proxy.invalid" in str(exc_info.value)


def test_the_whole_rendered_chain_is_clean_not_just_the_top_exception(monkeypatch, logger):
    # THE TEST ABOVE IS GREEN BY CONSTRUCTION AGAINST THIS BUG, which is why the leak
    # survived two reviews. `check_balance` re-raises, and with `from e` the ORIGINAL
    # exception stays attached as __cause__ carrying the full unredacted proxy string;
    # `str(exception)` never shows it, and every renderer of a chain does:
    # traceback.format_exc(), logger.error(..., exc_info=True), and the interpreter's
    # own dump of an exception that escapes (a crash, or Ctrl-C during a request) —
    # which goes to stderr, i.e. into `docker logs`.
    #
    # So this looks at the full render and nothing else. The fix is `from None`; what
    # keeps the message diagnosable is that the original's class name and its redacted
    # text are already inside the new message.
    class _LeakyGet:
        def __call__(self, url, proxies=None, timeout=None):
            raise ValueError(SOCKS_VERSION_ERROR)

    monkeypatch.setattr(src.balances.requests, "get", _LeakyGet())
    # Through a variable, not as a literal in the call: a rendered traceback prints
    # the SOURCE LINE of every frame, so a credential spelled out at the call site
    # would appear in the render no matter what the code does with it. Production
    # calls this as `check_balance(wallet.Address, logger, proxy)`; the test does the
    # same, otherwise it would be asserting against its own text.
    proxy = "socks9://puser:hunter2@proxy.invalid:1080"
    try:
        check_balance(ADDRESS, logger, proxy)
    except Exception as error:
        rendered = "".join(traceback.format_exception(
            type(error), error, error.__traceback__))
    else:
        raise AssertionError("check_balance did not raise")

    assert "hunter2" not in rendered, rendered
    assert "puser:hunter2@" not in rendered, rendered
    # And the diagnosis survives: the class of the original failure, its redacted
    # text, the proxy host and the wallet are all still in there.
    assert "ValueError" in rendered
    assert "proxy.invalid" in rendered
    assert ADDRESS in rendered


# --- describe_error ----------------------------------------------------------
#
# `str(exception)` alone is not a reason. A fair share of what this loop catches
# stringifies to nothing (`ConnectionError()`) or to almost nothing
# (`KeyError('price')` -> "'price'"), and the loop writes that text into the wallet's
# Grist `Comment` column — where `Error: ` is indistinguishable, weeks later, from a
# redaction that ate the whole message. The class name is not a secret.

def test_a_reason_is_the_class_name_in_front_of_the_redacted_text():
    reason = describe_error(ValueError(SOCKS_VERSION_ERROR))
    assert reason.startswith("ValueError: ")
    assert "hunter2" not in reason
    assert "proxy.invalid" in reason


def test_an_exception_with_no_text_at_all_still_produces_a_reason():
    # requests raises these bare more often than not.
    assert describe_error(requests.exceptions.ConnectionError()) == "ConnectionError"


def test_a_nearly_empty_reason_keeps_both_halves():
    # The purrfolio payloads are parsed by key, so a changed API answers with
    # KeyError('price') — "'price'" on its own says nothing about what happened.
    assert describe_error(KeyError("price")) == "KeyError: 'price'"


def test_a_silent_network_failure_reaches_the_caller_named(monkeypatch, logger):
    # End to end: the empty exception travels through check_balance, and both the log
    # line and the message the loop writes to Grist have to name it.
    class _SilentlyDead:
        def __call__(self, url, proxies=None, timeout=None):
            raise requests.exceptions.ConnectionError()

    monkeypatch.setattr(src.balances.requests, "get", _SilentlyDead())
    with pytest.raises(Exception) as exc_info:
        check_balance(ADDRESS, logger)
    assert "ConnectionError" in str(exc_info.value)
    assert any("ConnectionError" in message for message in logger.errors)


def test_a_missing_payload_key_is_reported_as_a_keyerror(monkeypatch, logger):
    # The realistic shape of "purrfolio changed its answer": the request succeeds and
    # the key is gone.
    class _EmptyPayload:
        def json(self):
            return {}

    monkeypatch.setattr(src.balances.requests, "get",
                        lambda url, proxies=None, timeout=None: _EmptyPayload())
    with pytest.raises(Exception) as exc_info:
        check_balance(ADDRESS, logger)
    assert "KeyError" in str(exc_info.value)
    assert "price" in str(exc_info.value)


# --- find_none_values --------------------------------------------------------

class _Wallet:
    def __init__(self, id, Address, hypercore_hype_value=None, hyperevm_hype_value=None):
        self.id = id
        self.Address = Address
        self.hypercore_hype_value = hypercore_hype_value
        self.hyperevm_hype_value = hyperevm_hype_value


class _FakeGrist:
    def __init__(self, wallets):
        self.wallets = wallets

    def fetch_table(self, table=None):
        # A fresh list every call: the function shuffles what it is handed, and a
        # shared list would make one test's ordering leak into the next.
        return list(self.wallets)


def test_only_wallets_with_an_address_and_a_gap_are_returned():
    grist = _FakeGrist([
        _Wallet(1, ""),                                        # no address at all
        _Wallet(2, None),                                      # ditto
        _Wallet(3, "0xaaa", 1.0, 2.0),                         # already complete
        _Wallet(4, "0xbbb", None, 2.0),                        # hypercore missing
        _Wallet(5, "0xccc", 1.0, ""),                          # hyperevm empty
        _Wallet(6, "0xddd", "", None),                         # both missing
    ])
    picked = find_none_values(grist, count=10)
    assert [wallet.id for wallet in picked] == [4, 5, 6]


def test_an_addressless_wallet_is_skipped_even_with_both_values_missing():
    # The address is what the lookup is performed on, so a row without one can
    # never be completed and must not consume a slot in the round.
    grist = _FakeGrist([_Wallet(1, ""), _Wallet(2, "0xaaa", None, None)])
    assert [wallet.id for wallet in find_none_values(grist, count=10)] == [2]


def test_the_result_is_cut_to_count():
    grist = _FakeGrist([_Wallet(n, "0x{}".format(n), None, None) for n in range(1, 11)])
    assert len(find_none_values(grist, count=3)) == 3


def test_count_defaults_to_one_wallet():
    grist = _FakeGrist([_Wallet(n, "0x{}".format(n), None, None) for n in range(1, 6)])
    assert len(find_none_values(grist)) == 1


def test_do_random_false_preserves_the_table_order():
    # The default path is deterministic; only the loop asks for shuffling. A
    # helper that shuffled unconditionally would make every test above flaky and
    # would hide an ordering bug behind "it is random anyway".
    grist = _FakeGrist([_Wallet(n, "0x{}".format(n), None, None) for n in range(1, 21)])
    assert [wallet.id for wallet in find_none_values(grist, count=20)] == list(range(1, 21))
    assert [wallet.id for wallet in find_none_values(grist, do_random=False, count=20)] == \
        list(range(1, 21))


def test_do_random_true_shuffles_both_before_and_after_filtering(monkeypatch):
    # Both shuffles are load-bearing and both are the original behaviour: without
    # the second one, a document with more pending wallets than `count` would
    # keep re-checking the same head of the list forever.
    shuffled = []

    def recording_shuffle(sequence):
        shuffled.append(len(sequence))

    monkeypatch.setattr(src.balances.random, "shuffle", recording_shuffle)
    grist = _FakeGrist([_Wallet(n, "0x{}".format(n), None, None) for n in range(1, 6)]
                       + [_Wallet(99, "0xdone", 1.0, 2.0)])
    find_none_values(grist, do_random=True, count=2)
    # First the whole fetched table (6 rows), then the filtered candidates (5).
    assert shuffled == [6, 5]
