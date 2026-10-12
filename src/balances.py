"""Wallet selection and the purrfolio.com balance lookup.

The arithmetic decides the numbers that land in the Grist document: the HYPE
price is fetched first and is the divisor for both values, so a change there
silently rescales every wallet checked afterwards.
"""

import random
import re
import uuid

import requests  # type: ignore


# `user:password@` in front of a host, with or without a scheme in front of it.
#
# Best effort, not hermetic: a regex over free-form exception text, and the strings
# it has to cope with are the ones that failed to parse as URLs. A password quoted
# without a following '@', split across lines or percent-encoded goes out in clear.
# Passwords are whatever the proxy vendor issued, so they may contain '/', '@' and
# whitespace ('/' is the most common cause of `Failed to parse:` itself).
#
# The width is bounded by two details that keep it from becoming a wildcard:
#
#   * the match is anchored on the LAST '@' of the run — `(?![^\s]*@)` requires the
#     host that follows to contain no further '@' — so a password that itself holds
#     '@' is swallowed whole instead of leaving its tail in the message;
#   * with a scheme the password may hold anything but a newline AND at most ONE run
#     of whitespace. That bound is what keeps `http://host:8080 refused for
#     admin@example.com` intact: without it, greedy prose between a `scheme://token:`
#     and any later '@' is eaten as a "password".
#
# Two alternatives rather than one optional scheme group:
#   * WITHOUT a scheme the password may not contain whitespace at all — otherwise
#     'Failed to parse: http://...' matches with `parse` as the user and the scheme
#     disappears into the redaction along with the password;
#   * `(?!//)` on that second alternative is what keeps this function IDEMPOTENT (it
#     runs at several layers — check_balance redacts, then the loop redacts the
#     result again): without it the already-redacted 'socks9://***@host' matches a
#     second time with `socks9` as the user and `//***` as the password.
#
# Over-redaction is the deliberate direction of the error: a proxy password in
# `docker logs` or a backed-up Grist cell is not recoverable.
_URL_CREDENTIALS = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)(?P<user>[^\s:/@]+):"
    r"(?P<password>[^\s]*?(?:[ \t][^\s]*?)??)@(?![^\s]*@)"
    r"|"
    r"(?P<bare_user>[^\s:/@]+):(?!//)(?P<bare_password>[^\s]*?)@(?![^\s]*@)")


def _mask_credentials(match):
    """`***@`, keeping the scheme when the matched form had one.

    A function rather than a `\\1***@` template, so the empty scheme of the
    second alternative is explicit rather than a side effect of `re.sub` rules.
    """
    return "{}***@".format(match.group("scheme") or "")


def redact_credentials(text):
    """Replace `user:password@` with `***@` in anything about to leave the process.

    The proxy string comes from the `Proxy` row of the Grist Settings table and in
    production it is a rotating residential proxy WITH credentials in it
    (`scheme://user:password@host:port`). Two library paths put that whole string,
    credentials included, into the text of the exception they raise — both
    reproduced against the pinned requests 2.32.3 / urllib3 2.2.3:

      * an unknown SOCKS scheme (`socks9://`, a typo in a spreadsheet cell) ->
        `ValueError: Unable to determine SOCKS version from socks9://user:pass@...`
        (urllib3/contrib/socks.py);
      * a proxy URL that does not parse -> `InvalidURL: Failed to parse: http://user:pass@[...`.

    `check_balance` logs and re-raises that text, and `src/checker.py` writes it
    into the wallet's `Comment` column, so it reaches `docker logs` and a backed-up
    Grist document.

    The host and port survive on purpose: they are what makes a proxy failure
    diagnosable, and they are not the secret.
    """
    if text is None:
        # Absent (a proxy setting that was never resolved, a None passed through a
        # log line) is not an error here: this function is called on the way OUT,
        # and it must never be the reason a log line or a Grist write fails.
        return ""
    return _URL_CREDENTIALS.sub(_mask_credentials, str(text))


def describe_error(error):
    """`ClassName: redacted text`, or bare `ClassName` when there is no text.

    Every "reason" this service reports is built through here. Many caught
    exceptions carry no message (`ConnectionError()` stringifies to '',
    `KeyError('price')` to "'price'"), and the class name is often the whole
    diagnosis (ConnectionError vs ProxyError vs KeyError), so it goes first.
    """
    text = redact_credentials(error)
    if not text:
        return type(error).__name__
    return "{}: {}".format(type(error).__name__, text)


def generate_proxy(proxy_string):
    """Fill the `{random_token}` placeholder with a fresh 10-char token.

    The placeholder pins a sticky residential proxy session, so a new token means
    a new exit IP. A string without the placeholder is returned unchanged — which
    means rotation is simply off, and the whole round goes out through one IP.
    """
    random_token = str(uuid.uuid4())[:10]
    return proxy_string.replace("{random_token}", random_token)


def check_balance(address, logger, proxy=None):
    """HYPE held by `address`, as (hypercore, hyperevm), via purrfolio.com.

    Three requests through the same proxy, and all three have to succeed: the
    price is the divisor for both returned values, so a partial answer would be
    written to Grist as a number rather than as a failure.

    The `re.sub` on each field is not decoration — these endpoints return values
    like "$1,234.56" as often as bare numbers, and `float()` on that raises a
    ValueError that says nothing about which of the three calls produced it.
    """
    hype_price_url = "https://purrfolio.com/api/hype-price"
    debank_url = "https://purrfolio.com/api/debank-data?address="
    hypercore_url = "https://purrfolio.com/api/hypercore-holdings?address="

    proxies = None
    if proxy:
        proxies = {'http': proxy, 'https': proxy}

    try:
        hype_price_response = requests.get(hype_price_url, proxies=proxies, timeout=10)
        hype_price = float(re.sub(r'[^\d.]', '', str(hype_price_response.json()["price"])))

        debank_response = requests.get(debank_url + address, proxies=proxies, timeout=10)
        debank_usd_value = float(re.sub(r'[^\d.]', '', str(debank_response.json()["usd_value"])))

        hypercore_response = requests.get(hypercore_url + address, proxies=proxies, timeout=10)
        hypercore_usd_value = float(re.sub(r'[^\d.]', '', str(hypercore_response.json()["grandTotal"])))

        hypercore_hype_value = hypercore_usd_value / hype_price
        hyperevm_hype_value = debank_usd_value / hype_price

        return hypercore_hype_value, hyperevm_hype_value

    except Exception as e:
        # Logged AND re-raised with the address in the text: the caller writes the
        # message into the wallet's own Grist row, so an error that does not name
        # the wallet is unattributable once several rounds have gone by.
        #
        # `from None`, not `from e`: with `from e` the original exception stays
        # attached as __cause__ with the unredacted proxy string, and every chain
        # renderer (format_exc, exc_info=True, an escaping exception) prints it into
        # `docker logs`. The original's class name survives via `describe_error`.
        reason = describe_error(e)
        logger.error(f"Error while checking token transactions for address {address}: {reason}")
        raise Exception(f"Error while checking token transactions for address {address}: {reason}") from None


def find_none_values(grist, table, do_random=False, count=1):
    """Up to `count` wallets that have an address and are still missing a value.

    Shuffled twice on purpose: the first spreads the fetch order, the second
    decides which of the pending wallets this round takes. Without the second one a document with more pending
    wallets than `count` would keep re-checking the same head of the list.
    """
    wallets = grist.fetch_table(table)
    if do_random:
        random.shuffle(wallets)
    wallets_non_empty_address = [wallet for wallet in wallets if (wallet.Address is not None and wallet.Address != "")]
    wallets_to_check = [wallet for wallet in wallets_non_empty_address if (wallet.hypercore_hype_value is None or wallet.hypercore_hype_value == "") or (wallet.hyperevm_hype_value is None or wallet.hyperevm_hype_value == "")]
    if do_random:
        random.shuffle(wallets_to_check)
    return wallets_to_check[:count]
