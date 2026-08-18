"""Wallet selection and the purrfolio.com balance lookup.

Moved out of the old top-level `airdrop_checker.py` with their logic unchanged.
The arithmetic below decides the numbers that land in the Grist document, so the
formulas and the ORDER of the three requests are kept exactly as they were: the
HYPE price is fetched first and is the divisor for both values, so a change there
silently rescales every wallet checked afterwards.

The jsonpath helpers that used to live beside these (`get_value_by_jsonpath`,
`parse_and_sum_jsonpaths`) and the single-wallet `find_none_value` did not come
along: nothing called them — the loop has used `find_none_values` since it
started taking several wallets per round. Dropping the two jsonpath ones is what
removed `jsonpath-ng` (and its `ply` dependency) from requirements.txt.
"""

import random
import re
import uuid

import requests  # type: ignore


# `user:password@` in front of a host, with or without a scheme in front of it.
#
# THIS IS BEST EFFORT AND NOT HERMETIC, and that is a property of the job rather
# than of this implementation: it is a regular expression over free-form exception
# text, not a URL parser, and the strings it has to cope with are exactly the ones
# that FAILED to parse as URLs — that is why urllib3 raised in the first place. A
# message that quotes the password without an '@' after it, that breaks it across
# two lines, or that shows it percent-encoded still goes out in the clear.
#
# What the previous, narrower pattern (`([^\s/@]*)` for the password) got wrong was
# not hypothetical either. The proxy string is typed by a person into a Grist cell,
# so the passwords that reach it are whatever the vendor handed them:
#
#   'Failed to parse: http://puser:hun/ter2@[proxy.invalid'      -> NOT MATCHED AT ALL
#   '...SOCKS version from socks9://puser:hun ter2@proxy...'     -> NOT MATCHED AT ALL
#   'http://user:p@sswOrd@host:8080'                             -> '***@sswOrd@host:8080'
#
# — i.e. it failed precisely on the case it exists for. A '/' in the password is the
# single most common reason that cell produces `Failed to parse:` at all, so the one
# message most likely to be logged was the one least likely to be redacted.
#
# Hence the width below, and the two details that keep it from becoming a wildcard:
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
# Over-redaction remains possible and is the deliberate direction of the error:
# eating part of a diagnostic message is recoverable, a proxy password in `docker
# logs` and in a Grist cell that people open and that goes into backups is not.
_URL_CREDENTIALS = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)(?P<user>[^\s:/@]+):"
    r"(?P<password>[^\s]*?(?:[ \t][^\s]*?)??)@(?![^\s]*@)"
    r"|"
    r"(?P<bare_user>[^\s:/@]+):(?!//)(?P<bare_password>[^\s]*?)@(?![^\s]*@)")


def _mask_credentials(match):
    """`***@`, keeping the scheme when the matched form had one.

    A function rather than a `\\1***@` template. The two are equivalent today —
    `re.sub` has substituted an empty string for an unmatched group since 3.5, and
    the scheme group is unmatched for every match of the second alternative — but
    the template says so by accident, through a rule most readers would have to look
    up, while this says it on purpose.
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

    That text does not stay in the process: `check_balance` logs it and re-raises
    it, and `src/checker.py` writes the caught exception into the wallet's
    `Comment` column. A password would therefore end up in `docker logs` AND in a
    Grist document that people open and that gets backed up — where rotating it
    means rewriting cells, not just an env var.

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

    Every "reason" this service reports is built through here rather than from
    `str(exception)` alone, and the missing half is the class name. A good share of
    the exceptions this loop actually catches carry no message at all —
    `requests.exceptions.ConnectionError()` stringifies to '' and `KeyError('price')`
    to just "'price'" — so the old formatting produced `Error occurred: ` in the log
    and wrote `Error: ` into the wallet's Grist `Comment` column.

    That matters more in the cell than in the log: the log line is gone with the next
    rotation, the cell is read weeks later by a person who cannot tell "the request
    failed with a silent exception" from "the redaction ate the whole message". The
    class name is not a secret and is often the entire diagnosis (ConnectionError vs
    ProxyError vs KeyError), so it goes in front and the redacted text follows it.
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
        # `from None`, NOT `from e`, and this is the whole point of the line.
        # Redacting the new message only cleans the TOP of the chain: with `from e`
        # the original exception stays attached as __cause__ with the full proxy
        # string — password included — inside it, and every renderer of a chain
        # prints that second half verbatim. `traceback.format_exc()`,
        # `logger.error(..., exc_info=True)`, and the interpreter's own dump of an
        # escaping exception (a crash, or Ctrl-C during a request) all go to stderr,
        # i.e. straight into `docker logs`. A test on `str(exception)` is green
        # against all of that by construction, which is exactly why the leak
        # survived earlier reviews; the test that covers it now renders the whole
        # chain with `traceback.format_exception`.
        #
        # What is given up is the original's own traceback frames — which of the
        # three purrfolio calls raised. What replaces it: `describe_error` puts the
        # original's CLASS NAME in front of its redacted text, so the message still
        # says ConnectionError vs ProxyError vs KeyError, the address still names
        # the wallet, and the line below logs the same thing at the point of
        # failure, where the surrounding log lines say which round it belongs to.
        reason = describe_error(e)
        logger.error(f"Error while checking token transactions for address {address}: {reason}")
        raise Exception(f"Error while checking token transactions for address {address}: {reason}") from None


def find_none_values(grist, table=None, do_random=False, count=1):
    """Up to `count` wallets that have an address and are still missing a value.

    Shuffled twice on purpose, and both shuffles are the original behaviour: the
    first spreads the fetch order, the second decides WHICH of the pending
    wallets this round takes. Without the second one a document with more pending
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
