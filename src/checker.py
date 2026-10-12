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
from grist_client import GristClient

from src.balances import (
    check_balance,
    describe_error,
    find_none_values,
    generate_proxy,
    redact_credentials,
)
from src.heartbeat import write_heartbeat
from src.settings import settings

# getLogger() has no side effect; process-wide setup (handler, colorama) happens in
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
# Load-bearing: the pause between rounds comes from Grist in minutes, and a single
# sleep longer than HEARTBEAT_MAX_AGE would get a healthy container auto-healed.
HEARTBEAT_SLEEP_CHUNK = 30  # seconds


def _configure_process():
    """Process-wide setup, done once when the loop starts — never at import.

    colorama replaces `sys.stdout` and the handler writes to stderr; the tests and
    `ci/smoke.py` import this module without running the loop.
    """
    colorama.init(autoreset=True)

    logger.setLevel(logging.INFO)
    # The tests call run() several times; the guard keeps handlers from stacking.
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def _write_heartbeat():
    write_heartbeat(HEARTBEAT_FILE, logger=logger)


def sleep_with_heartbeat(total_seconds):
    """Sleep `total_seconds`, refreshing the liveness mark every chunk.

    Used for every sleep in the loop, including the ten-second error pauses.
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

    grist = GristClient(settings.grist_server, settings.grist_doc_id, settings.grist_api_key)

    # The first mark, before the first Grist call: the deploy waits for `healthy`
    # within max(120s, start_period + 15s) and rolls back otherwise, and a slow
    # Grist can make the first round longer than that.
    _write_heartbeat()

    while True:
        _write_heartbeat()                     # liveness mark each iteration
        try:
            proxy_string = grist.get_setting(SETTINGS_TABLE, "Proxy")
            random.seed(datetime.now().timestamp())
            # `Walled count` matches the typo in the Grist document's Setting
            # column; get_setting raises on an unknown name, so do not "fix" it.
            wallet_count_max = int(grist.get_setting(SETTINGS_TABLE, "Walled count max"))
            wallet_count_min = int(grist.get_setting(SETTINGS_TABLE, "Walled count min"))
            wait_time_max = int(grist.get_setting(SETTINGS_TABLE, "Wait time max"))
            wait_time_min = int(grist.get_setting(SETTINGS_TABLE, "Wait time min"))
            logger.info(f"wallet_count_max: {wallet_count_max}, wallet_count_min: {wallet_count_min}, wait_time_max: {wait_time_max}, wait_time_min: {wait_time_min}")
            wallets_count = random.randint(wallet_count_min, wallet_count_max)
            wallets = find_none_values(grist, NODES_TABLE, do_random=True, count=wallets_count)
            # The settings and wallet fetches above are six HTTP calls; re-mark
            # before the per-wallet work on a slow Grist.
            _write_heartbeat()
            try:
                proxy = generate_proxy(proxy_string)
                if wallets is None or len(wallets) == 0:
                    logger.info("No wallets to check, sleep 10s")
                    sleep_with_heartbeat(10)
                    continue
                for wallet in wallets:
                    # A mark per wallet: the round size is the operator's Grist
                    # setting and each wallet costs three proxied requests plus a
                    # Grist write, so a long round must not look hung.
                    _write_heartbeat()
                    try:
                        # Redacted on the happy path too: the proxy string carries
                        # `user:password@`.
                        logger.info(f"Check wallet {wallet.Address} with proxy {redact_credentials(proxy)}...")
                        hypercore_hype_value, hyperevm_hype_value = check_balance(wallet.Address, logger, proxy)
                        grist.update(NODES_TABLE, wallet.id, {"hypercore_hype_value": hypercore_hype_value, "hyperevm_hype_value": hyperevm_hype_value})
                    except Exception as e:
                        # Logged and written into the wallet's Grist row; a proxy
                        # failure quotes the proxy URL with credentials, and
                        # `describe_error` keeps the class name for messageless
                        # exceptions.
                        reason = describe_error(e)
                        logger.error(f"Error occurred: {reason}")
                        # Known risk: this writes `Value` and `Comment`, columns
                        # nothing else touches. If the Wallets table lacks them,
                        # Grist answers 400, this raises inside the except block and
                        # the round dies on the outer handler. The document's owner
                        # decides the column names.
                        grist.update(NODES_TABLE, wallet.id, {"Value": "--", "Comment": f"Error: {reason}"})
            except Exception as e:
                # The traceback is redacted too: `from None` in check_balance only
                # cleans that one chain, and the Grist calls go through `requests`,
                # which honours HTTP(S)_PROXY and quotes the proxy URL in a
                # ProxyError.
                logger.error(f"Error occurred: {describe_error(e)}")
                logger.error(f"Fail: {describe_error(e)}\n{redact_credentials(traceback.format_exc())}")
                sleep_with_heartbeat(10)
                continue

            time_to_sleep = random.uniform(wait_time_min*60, wait_time_max*60)
            logger.info(f"Sleep {time_to_sleep/60} minutes")
            sleep_with_heartbeat(time_to_sleep)
        except Exception as e:
            # The outermost net, and the one that catches the settings fetches: those
            # go out through `requests` inside grist_client, so a broken HTTP(S)_PROXY
            # in the stack's environment arrives here as a GristError quoting the
            # ProxyError, the whole proxy URL included.
            logger.error(f"Error occurred, sleep 10s: {describe_error(e)}")
            sleep_with_heartbeat(10)
