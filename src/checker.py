#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# flake8: noqa
# pylint: disable=broad-exception-raised, raise-missing-from, too-many-arguments, redefined-outer-name
# pylint: disable=multiple-statements, logging-fstring-interpolation, trailing-whitespace, line-too-long
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring
# pylint: disable=f-string-without-interpolation, wrong-import-position
# pylance: disable=reportMissingImports, reportMissingModuleSource

"""The single-process loop: read wallets from Grist, ask purrfolio, write back."""

import logging
import random
import time
import traceback
from datetime import datetime

import colorama  # type: ignore

from src.balances import (
    check_balance,
    describe_error,
    find_none_values,
    generate_proxy,
    redact_credentials,
)
from src.grist import GRIST
from src.heartbeat import write_heartbeat
from src.http_timeout import install_default_timeout
from src.settings import settings

# Naming the logger is not a side effect — getLogger() only registers a name, and
# `_write_heartbeat` below needs the object. Everything that CHANGES process-wide
# state (the handler, colorama's stdout wrapper, the requests patch) happens in
# _configure_process(), called from run().
logger = logging.getLogger("airdrop_checker")

# The tables of the Grist document this service works against. Not configurable:
# the column names the loop reads and writes below are specific to these two.
NODES_TABLE = "Wallets"
SETTINGS_TABLE = "Settings"

# The liveness mark src/healthcheck.py reads (Docker HEALTHCHECK), so a genuinely
# hung loop is detected and the autoheal-labelled container is restarted.
HEARTBEAT_FILE = settings.heartbeat_file

# How long a single stretch of sleeping may last before the mark is refreshed.
# THIS IS THE LOAD-BEARING NUMBER OF THE WHOLE HEALTHCHECK, not a tuning knob:
# the pause between rounds is read from Grist in MINUTES ("Wait time min/max"),
# so one `time.sleep(time_to_sleep)` would leave the heartbeat untouched for as
# long as the operator typed into a spreadsheet cell. Past HEARTBEAT_MAX_AGE the
# probe calls that healthy container unhealthy and auto-heal restarts it —
# on a schedule, forever, for doing exactly what it was configured to do. Sleeping
# in 30 s pieces and re-marking after each one makes a long pause indistinguishable
# from work, which is what it should have been all along.
HEARTBEAT_SLEEP_CHUNK = 30  # seconds


def _configure_process():
    """Process-wide setup, done once when the loop starts — never at import.

    All three of these reach outside this module: colorama replaces `sys.stdout`
    with a wrapper, the handler makes this logger write to stderr, and the
    timeout patch rewrites `requests.Session.request` for everything in the
    process. Doing them at import time means merely IMPORTING `src.checker` —
    which the test suite and `ci/smoke.py` both do, without any intention of
    running the loop — silently reshapes stdout and the HTTP library for whoever
    imported it. A program's side effects belong to running it.
    """
    # FIRST in this function, which run() calls before it builds the Grist client:
    # grist_api issues its requests without a timeout, so a stalled Grist
    # connection hangs this process forever. A patch installed after the first
    # fetch protects every call except the one that is already hanging.
    install_default_timeout()

    colorama.init(autoreset=True)

    logger.setLevel(logging.INFO)
    # run() is called once per process, so the guard is not for production: it
    # keeps a second call (the tests drive run() several times) from stacking
    # handlers and doubling every log line.
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def _write_heartbeat():
    write_heartbeat(HEARTBEAT_FILE, logger=logger)


def sleep_with_heartbeat(total_seconds):
    """Sleep `total_seconds`, refreshing the liveness mark every chunk.

    Used for EVERY sleep in the loop, not just the long one — the short
    ten-second error pauses go through it too, so there is only one way to sleep
    here and no second path that could quietly forget the mark.
    """
    remaining = float(total_seconds)
    while remaining > 0:
        chunk = min(HEARTBEAT_SLEEP_CHUNK, remaining)
        time.sleep(chunk)
        remaining -= chunk
        _write_heartbeat()


def run():
    """The main loop. Fetch the round's settings from Grist, check some wallets, sleep."""

    _configure_process()

    grist = GRIST(settings.grist_server, settings.grist_doc_id, settings.grist_api_key,
                  NODES_TABLE, SETTINGS_TABLE, logger)

    # The first mark, written BEFORE the first Grist call. It says "the process
    # started and its configuration parsed", which is precisely what the deploy
    # needs to hear: our Portainer build waits for `healthy` within
    # max(120s, start_period + 15s) after recreating the container and rolls the
    # image back otherwise, and the first round can easily be slower than that
    # window if Grist is having a bad day.
    _write_heartbeat()

    while True:
        _write_heartbeat()                     # liveness mark each iteration
        try:
            proxy_string = grist.find_settings("Proxy")
            random.seed(datetime.now().timestamp())
            # `Walled count` is a typo in the Grist document's own Setting column.
            # It is spelled that way HERE because it is spelled that way THERE —
            # the document belongs to someone else, and find_settings raises on a
            # name it cannot find, so "fixing" this string stops the service.
            wallet_count_max = int(grist.find_settings("Walled count max"))
            wallet_count_min = int(grist.find_settings("Walled count min"))
            wait_time_max = int(grist.find_settings("Wait time max"))
            wait_time_min = int(grist.find_settings("Wait time min"))
            logger.info(f"wallet_count_max: {wallet_count_max}, wallet_count_min: {wallet_count_min}, wait_time_max: {wait_time_max}, wait_time_min: {wait_time_min}")
            wallets_count = random.randint(wallet_count_min, wallet_count_max)
            wallets = find_none_values(grist, do_random=True, count=wallets_count)
            # Every line above this one is network: five find_settings calls, each
            # fetching the Settings table over HTTP, and then the Wallets fetch
            # inside find_none_values. On a slow Grist the mark at the top of the
            # iteration is already old by the time execution reaches here, so the
            # round is re-marked before the per-wallet work begins.
            _write_heartbeat()
            try:
                proxy = generate_proxy(proxy_string)
                if wallets is None or len(wallets) == 0:
                    logger.info("No wallets to check, sleep 10s")
                    sleep_with_heartbeat(10)
                    continue
                for wallet in wallets:
                    # A progress mark per wallet, and it is the load-bearing one
                    # for a busy round. How many wallets a round takes is
                    # `Walled count max` in the Grist Settings table — the
                    # operator's number, not the code's — and each wallet costs
                    # three purrfolio requests through a proxy plus a Grist write.
                    # Without this the whole round is one unmarked stretch, and a
                    # round longer than HEARTBEAT_MAX_AGE gets a perfectly healthy
                    # service restarted by auto-heal in the middle of its work,
                    # then again on the next round, forever. A long round is as
                    # normal a phase of this service as a long pause, and the
                    # probe has to answer "healthy" during both.
                    _write_heartbeat()
                    try:
                        # The proxy is redacted even on the happy path: the string
                        # comes from Grist with `user:password@` in it, and this
                        # line runs once per wallet, so an unredacted one puts the
                        # password in `docker logs` on every single round.
                        logger.info(f"Check wallet {wallet.Address} with proxy {redact_credentials(proxy)}...")
                        hypercore_hype_value, hyperevm_hype_value = check_balance(wallet.Address, logger, proxy)
                        grist.update(wallet.id, {"hypercore_hype_value": hypercore_hype_value, "hyperevm_hype_value": hyperevm_hype_value})
                    except Exception as e:
                        # Redacted on the way out in both directions: this text is
                        # logged AND written into the wallet's Grist row below, and
                        # a proxy failure carries the proxy URL — credentials
                        # included — in its message. A password in a Grist cell
                        # outlives the log: people open that document and it goes
                        # into backups.
                        #
                        # `describe_error` and not `redact_credentials` alone: the
                        # `Comment` cell is read long after the log is gone, and
                        # several of the exceptions that reach here stringify to
                        # nothing at all (`ConnectionError()`), which used to write
                        # a bare `Error: ` — indistinguishable, weeks later, from a
                        # redaction that ate the whole message.
                        reason = describe_error(e)
                        logger.error(f"Error occurred: {reason}")
                        # KNOWN RISK, deliberately left as it was found. The
                        # success path above writes `hypercore_hype_value` /
                        # `hyperevm_hype_value`; this failure path writes `Value`
                        # and `Comment` instead — two columns nothing else in this
                        # repository touches. If the Wallets table does not have
                        # them, Grist rejects the whole batch with 400 and this
                        # update raises INSIDE the except block, so the wallet's
                        # failure is replaced by a second, unrelated one and the
                        # round dies on the outer handler. Whether those columns
                        # exist is a property of a document this repository does
                        # not own, so changing the names is the owner's call, not
                        # a refactor's.
                        grist.update(wallet.id, {"Value": "--", "Comment": f"Error: {reason}"})
            except Exception as e:
                # The traceback goes through the redaction too, not just the
                # message, and it stays that way now that `check_balance` re-raises
                # with `from None`. That suppression cleans the ONE chain this
                # module builds itself; format_exc() here renders whatever exception
                # actually arrived, and the loop's own Grist calls go out through
                # `requests`, which honours HTTP_PROXY/HTTPS_PROXY and puts the
                # whole proxy URL into the text of a ProxyError. So a chain reaching
                # this handler can still carry credentials that nothing upstream of
                # it ever touched.
                logger.error(f"Error occurred: {describe_error(e)}")
                logger.error(f"Fail: {describe_error(e)}\n{redact_credentials(traceback.format_exc())}")
                sleep_with_heartbeat(10)
                continue

            time_to_sleep = random.uniform(wait_time_min*60, wait_time_max*60)
            logger.info(f"Sleep {time_to_sleep/60} minutes")
            sleep_with_heartbeat(time_to_sleep)
        except Exception as e:
            # The outermost net, and the one that catches the settings fetches: those
            # go out through `requests`, so a broken HTTP(S)_PROXY in the stack's
            # environment arrives here as a ProxyError quoting the whole proxy URL.
            logger.error(f"Error occurred, sleep 10s: {describe_error(e)}")
            sleep_with_heartbeat(10)
