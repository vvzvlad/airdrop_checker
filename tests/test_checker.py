"""Sleeping in pieces: the property the whole HEALTHCHECK rests on.

The pause between rounds is read from Grist in minutes; a single sleep longer than
HEARTBEAT_MAX_AGE would get a healthy container restarted by auto-heal.

`time.sleep` is replaced throughout, so these tests take no real time.
"""

import requests
from grist_client import GristError

import src.checker
from src.checker import HEARTBEAT_SLEEP_CHUNK, sleep_with_heartbeat
from src.heartbeat import DEFAULT_HEARTBEAT_MAX_AGE


class _Recorder:
    """Captures every sleep and every heartbeat, in order."""

    def __init__(self):
        self.slept = []
        self.marks = 0

    def sleep(self, seconds):
        self.slept.append(seconds)

    def write_heartbeat(self, path, logger=None):
        self.marks += 1


def _patch(monkeypatch):
    recorder = _Recorder()
    monkeypatch.setattr(src.checker.time, "sleep", recorder.sleep)
    monkeypatch.setattr(src.checker, "write_heartbeat", recorder.write_heartbeat)
    return recorder


def test_a_long_pause_produces_many_marks_rather_than_one(monkeypatch):
    # Ten minutes is an ordinary value for "Wait time max" in the Grist Settings
    # table. One mark for the whole stretch is exactly the state the probe reads
    # as a hung process.
    recorder = _patch(monkeypatch)
    sleep_with_heartbeat(600)
    assert recorder.marks == 20
    assert recorder.marks > 1


def test_no_single_stretch_of_sleep_exceeds_the_chunk(monkeypatch):
    # The mark's age never exceeds one chunk while the loop is sleeping, whatever
    # the operator typed.
    recorder = _patch(monkeypatch)
    sleep_with_heartbeat(3600)
    assert max(recorder.slept) <= HEARTBEAT_SLEEP_CHUNK


def test_the_chunks_add_up_to_the_requested_pause(monkeypatch):
    # Chunking must not shorten (or lengthen) the wait: the pause is a rate limit
    # against purrfolio, not a formality.
    recorder = _patch(monkeypatch)
    sleep_with_heartbeat(95)
    assert sum(recorder.slept) == 95
    assert recorder.slept == [30, 30, 30, 5]
    assert recorder.marks == 4


def test_a_short_pause_still_sleeps_once_and_marks_once(monkeypatch):
    # The ten-second error pauses go through the same function — there is only one
    # way to sleep in this loop, so no second path can quietly forget the mark.
    recorder = _patch(monkeypatch)
    sleep_with_heartbeat(10)
    assert recorder.slept == [10]
    assert recorder.marks == 1


def test_a_fractional_pause_is_honoured(monkeypatch):
    # random.uniform() produces floats, and an int-only implementation would round
    # them into a busy loop or into no sleep at all.
    recorder = _patch(monkeypatch)
    sleep_with_heartbeat(45.5)
    assert sum(recorder.slept) == 45.5
    assert recorder.marks == 2


def test_zero_and_negative_pauses_do_nothing(monkeypatch):
    # A "Wait time" of 0 is legal in the settings table; it must not spin.
    recorder = _patch(monkeypatch)
    sleep_with_heartbeat(0)
    sleep_with_heartbeat(-5)
    assert recorder.slept == []
    assert recorder.marks == 0


def test_the_chunk_is_far_below_the_probes_freshness_window():
    # The relation that makes the chunking worth anything: if the chunk ever grew
    # past the window, a sleeping loop would go unhealthy between two marks.
    assert HEARTBEAT_SLEEP_CHUNK < DEFAULT_HEARTBEAT_MAX_AGE


# --- run(): the loop itself ---------------------------------------------------
#
# Everything above tests the sleeping helper; none of it ever entered `run()`, so
# the properties the whole HEALTHCHECK rests on — WHERE the marks are written and
# HOW the loop sleeps — were pinned nowhere. These drive the real `run()` with
# Grist, the balance lookup and both sleep functions replaced, and end the
# otherwise infinite loop by raising from a stub on the Nth iteration.


class _StopTheLoop(BaseException):
    """Ends the loop from inside a stub.

    A BaseException on purpose: `run()` wraps its body in `except Exception` and
    would otherwise swallow the signal, sleep ten seconds and carry on — so a
    plain exception could not stop the loop at all, and a test using one would
    hang instead of failing.
    """


def _as_error(failure, default_message):
    """What a `fail_*` switch should raise.

    `True` -> a plain error with the default text; a string -> that text; an
    exception instance -> itself, so a test can hand the loop an exception with a
    __cause__ already attached and see what a rendered chain does to it.
    """
    if isinstance(failure, BaseException):
        return failure
    if isinstance(failure, str):
        return RuntimeError(failure)
    return RuntimeError(default_message)


class _RecordingLogger:
    """The module logger, replaced by something that keeps every line.

    Every redaction in `src/checker.py` ends in a log call. Replaces the real logger
    so assertions see the exact strings, before any handler or formatter.
    """

    def __init__(self):
        self.messages = []
        # `_configure_process()` adds a StreamHandler when this list is empty; it is
        # left empty so that code path runs exactly as it does in production. The
        # handler it appends here never sees anything: the methods below record and
        # return.
        self.handlers = []

    def setLevel(self, level):
        pass

    def addHandler(self, handler):
        self.handlers.append(handler)

    def info(self, message):
        self.messages.append(message)

    def error(self, message):
        self.messages.append(message)

    def warning(self, message):
        self.messages.append(message)

    def leaking(self, secret):
        """Every recorded line that contains `secret`."""
        return [message for message in self.messages if secret in message]


class _FakeGrist:
    """Records every Grist touch, and can be made to fail on any of them."""

    def __init__(self, events, settings_values, iterations, fail_get_setting=False,
                 fail_update=False):
        self.events = events
        self.settings_values = settings_values
        self.iterations = iterations
        self.turns = 0
        self.fail_get_setting = fail_get_setting
        self.fail_update = fail_update
        self.updates = []
        events.append(("grist_init",))

    def get_setting(self, table, setting):
        # The "Proxy" lookup is the first statement of every iteration, so it is
        # where the turns are counted — and counting them HERE rather than further
        # down the round is what lets the error-branch tests terminate: those
        # never reach the wallet work at all.
        if setting == "Proxy":
            self.turns += 1
            if self.turns > self.iterations:
                raise _StopTheLoop()
        self.events.append(("settings", setting))
        if self.fail_get_setting:
            raise _as_error(self.fail_get_setting, "Grist is unreachable")
        return self.settings_values[setting]

    def update(self, table, row_id, updates):
        self.events.append(("update", row_id, tuple(sorted(updates))))
        self.updates.append((row_id, dict(updates)))
        if self.fail_update:
            raise _as_error(self.fail_update, "Grist rejected the batch")

    def fetch_table(self, table):
        self.events.append(("fetch_table",))
        return []


class _Wallet:
    def __init__(self, id, Address):
        self.id = id
        self.Address = Address


class _Harness:
    """The whole outside world of `run()`, recorded in one ordered event list."""

    def __init__(self):
        self.events = []
        self.grist = None
        self.logger = _RecordingLogger()

    def kinds(self):
        return [event[0] for event in self.events]


def _drive_run(monkeypatch, wallets=(), iterations=1, fail_get_setting=False,
               fail_check_balance=None, fail_update=False, fail_generate_proxy=None):
    """Run `run()` for `iterations` turns and return the recorded events.

    Every boundary the loop has is replaced: the Grist client, wallet selection,
    the balance lookup, the proxy preparation, BOTH sleep functions, the heartbeat
    writer, the module logger and the process setup that would otherwise wrap stdout
    through colorama.

    Each `fail_*` switch takes True, a message, or a ready exception object (see
    `_as_error`) — the last of those is what lets a test reproduce the exception
    SHAPE `check_balance` produces, redacted text on top of an unredacted __cause__.

    `fail_generate_proxy` reaches the round handler without the failing
    `Value`/`Comment` write, whose column names are the Grist document owner's call;
    `generate_proxy` is the first statement of the round body.
    """
    harness = _Harness()
    events = harness.events
    settings_values = {
        "Proxy": "socks5://user:hunter2@proxy.invalid:1080",
        "Walled count max": "2",
        "Walled count min": "2",
        "Wait time max": "2",
        "Wait time min": "1",
    }

    def fake_grist_factory(*args, **kwargs):
        harness.grist = _FakeGrist(events, settings_values, iterations,
                                   fail_get_setting=fail_get_setting,
                                   fail_update=fail_update)
        return harness.grist

    def fake_find_none_values(grist, table, do_random=False, count=1):
        events.append(("wallets", count))
        return list(wallets)

    def fake_check_balance(address, logger, proxy=None):
        events.append(("check", address))
        if fail_check_balance is not None:
            raise _as_error(fail_check_balance, "balance lookup failed")
        return 1.0, 2.0

    real_generate_proxy = src.checker.generate_proxy

    def fake_generate_proxy(proxy_string):
        events.append(("proxy",))
        if fail_generate_proxy is not None:
            raise _as_error(fail_generate_proxy, "the proxy setting is unusable")
        # Otherwise the real one: the token substitution is what puts a proxy STRING
        # in front of the wallet loop, and the redaction tests read that string.
        return real_generate_proxy(proxy_string)

    def fake_sleep_with_heartbeat(seconds):
        # Deliberately does NOT call time.sleep: a raw `time.sleep` recorded below
        # can then only have come from the loop's own body.
        events.append(("sleep_hb", seconds))

    def fake_time_sleep(seconds):
        events.append(("time.sleep", seconds))

    def fake_write_heartbeat(path, logger=None):
        events.append(("mark",))

    class _FakeColorama:
        @staticmethod
        def init(*args, **kwargs):
            events.append(("colorama_init",))

    monkeypatch.setattr(src.checker, "GristClient", fake_grist_factory)
    monkeypatch.setattr(src.checker, "find_none_values", fake_find_none_values)
    monkeypatch.setattr(src.checker, "check_balance", fake_check_balance)
    monkeypatch.setattr(src.checker, "generate_proxy", fake_generate_proxy)
    monkeypatch.setattr(src.checker, "sleep_with_heartbeat", fake_sleep_with_heartbeat)
    monkeypatch.setattr(src.checker.time, "sleep", fake_time_sleep)
    monkeypatch.setattr(src.checker, "write_heartbeat", fake_write_heartbeat)
    monkeypatch.setattr(src.checker, "colorama", _FakeColorama)
    monkeypatch.setattr(src.checker, "logger", harness.logger)

    try:
        src.checker.run()
    except _StopTheLoop:
        pass
    return harness


def _index(events, predicate):
    for position, event in enumerate(events):
        if predicate(event):
            return position
    raise AssertionError("no such event in {!r}".format(events))


def test_the_first_mark_is_written_before_the_first_grist_call(monkeypatch):
    # The deploy depends on this one: our Portainer build waits for `healthy`
    # within max(120s, start_period + 15s) after recreating the container and
    # rolls the image back otherwise. A first mark written only after Grist
    # answered would make a slow (or unreachable) Grist look like a bad image.
    events = _drive_run(monkeypatch, iterations=1).events
    first_mark = _index(events, lambda event: event[0] == "mark")
    first_grist_call = _index(events, lambda event: event[0] in ("settings", "fetch_table"))
    assert first_mark < first_grist_call
    # TWO marks before that first call, and the count is the assertion. The
    # startup mark and the first iteration's mark are indistinguishable in this
    # log — same call, adjacent positions — so anything weaker than counting them
    # is satisfied by either one alone, and the startup mark could be deleted
    # without a single test noticing. It is the one that keeps a container inside
    # the deploy's `healthy` window when the first round is slow, so it is the one
    # worth counting.
    marks_before = [event for event in events[:first_grist_call] if event == ("mark",)]
    assert len(marks_before) >= 2, \
        "only {} mark(s) before the first Grist call: {!r}".format(
            len(marks_before), events[:first_grist_call + 1])


def test_every_iteration_starts_with_a_mark(monkeypatch):
    # Three turns, so this cannot pass on the strength of the startup mark alone.
    events = _drive_run(monkeypatch, iterations=3).events
    starts = [position for position, event in enumerate(events)
              if event == ("settings", "Proxy")]
    assert len(starts) == 3
    for position in starts:
        assert events[position - 1] == ("mark",), \
            "iteration at {} does not open with a heartbeat: {!r}".format(
                position, events[max(0, position - 4):position + 1])


def test_the_round_is_marked_between_the_settings_block_and_the_wallets(monkeypatch):
    # Five get_setting calls plus the wallet fetch are six HTTP round trips, and
    # then the per-wallet work begins. Two marks have to sit between the wallet
    # selection and the first balance lookup: one closing the settings block, one
    # opening the first wallet.
    wallets = [_Wallet(1, "0xaaa"), _Wallet(2, "0xbbb")]
    events = _drive_run(monkeypatch, wallets=wallets, iterations=1).events
    wallets_at = _index(events, lambda event: event[0] == "wallets")
    first_check = _index(events, lambda event: event[0] == "check")
    marks_between = [event for event in events[wallets_at:first_check]
                     if event == ("mark",)]
    assert len(marks_between) >= 2, \
        "only {} mark(s) between the wallet fetch and the first lookup: {!r}".format(
            len(marks_between), events[wallets_at:first_check + 1])


def test_every_wallet_is_marked_before_it_is_checked(monkeypatch):
    # The round's length is `Walled count max` in a spreadsheet — the operator's
    # number, not the code's — and every wallet costs three proxied requests plus
    # a Grist write. Without a mark per wallet a long round is one silent stretch,
    # and auto-heal restarts a healthy service in the middle of its work.
    wallets = [_Wallet(1, "0xaaa"), _Wallet(2, "0xbbb"), _Wallet(3, "0xccc")]
    events = _drive_run(monkeypatch, wallets=wallets, iterations=1).events
    checks = [position for position, event in enumerate(events) if event[0] == "check"]
    assert len(checks) == 3
    for position in checks:
        assert events[position - 1] == ("mark",), \
            "wallet at {} is checked without a fresh mark: {!r}".format(
                position, events[max(0, position - 3):position + 1])


def test_the_loop_never_sleeps_through_time_sleep_directly(monkeypatch):
    # Every branch, including both `continue`s, must sleep through
    # sleep_with_heartbeat. A bare time.sleep(10) looks harmless and is not: it is
    # a stretch with no mark, and it is how the chunking gets quietly undone.
    wallets = [_Wallet(1, "0xaaa")]
    for kwargs in ({}, {"wallets": wallets},
                   {"fail_get_setting": True},
                   {"wallets": wallets, "fail_check_balance": "boom", "fail_update": True}):
        events = _drive_run(monkeypatch, iterations=2, **kwargs).events
        assert not [event for event in events if event[0] == "time.sleep"], \
            "the loop called time.sleep directly with {!r}: {!r}".format(kwargs, events)
        assert [event for event in events if event[0] == "sleep_hb"], \
            "no sleep at all happened with {!r}".format(kwargs)


def test_a_failing_grist_fetch_sleeps_ten_seconds_with_heartbeats_and_retries(monkeypatch):
    # The outer handler. A service whose Grist is down keeps marking itself alive,
    # which is correct: it is a service problem, not a hung process, and
    # restarting it would not help.
    events = _drive_run(monkeypatch, iterations=2, fail_get_setting=True).events
    assert ("sleep_hb", 10) in events
    assert len([event for event in events if event == ("settings", "Proxy")]) >= 2


def test_an_empty_round_sleeps_ten_seconds_with_heartbeats(monkeypatch):
    # The "No wallets to check" branch ends in `continue`, which is exactly the
    # kind of path a raw time.sleep gets left on.
    events = _drive_run(monkeypatch, wallets=(), iterations=2).events
    assert ("sleep_hb", 10) in events


def test_a_wallet_failure_that_also_fails_to_be_recorded_sleeps_with_heartbeats(monkeypatch):
    # The known-risk path: the per-wallet handler writes `Value`/`Comment`, that
    # write raises inside the except block, and the round dies on the inner
    # handler. Its `continue` has to go through sleep_with_heartbeat too.
    wallets = [_Wallet(1, "0xaaa")]
    events = _drive_run(monkeypatch, wallets=wallets, iterations=2,
                        fail_check_balance="boom", fail_update=True).events
    assert ("sleep_hb", 10) in events


def test_a_completed_round_sleeps_the_grist_pause_through_the_chunked_sleep(monkeypatch):
    # "Wait time min/max" are 1 and 2 minutes in the harness, so the pause lands
    # between 60 and 120 seconds — and it must arrive at sleep_with_heartbeat,
    # which is what turns it into 30-second pieces.
    wallets = [_Wallet(1, "0xaaa")]
    events = _drive_run(monkeypatch, wallets=wallets, iterations=1).events
    pauses = [event[1] for event in events if event[0] == "sleep_hb"]
    assert any(60 <= pause <= 120 for pause in pauses), pauses


# --- what the loop puts in the log -------------------------------------------
#
# Four call sites in `src/checker.py` redact before logging; these check the text,
# since executing the lines does not prove the password is absent.

PROXY_PASSWORD = "hunter2"
PROXY_URL = "socks5://user:hunter2@proxy.invalid:1080"
# What has to survive the redaction in the round and outermost handlers, where the
# line is the whole record of the failure: "no password" alone is satisfied by a line
# that says nothing. Host and port: the port names the exit on a multi-exit vendor.
PROXY_HOST = "proxy.invalid:1080"
# The shape urllib3 produces for a proxy whose scheme it cannot read — the message
# quotes the whole URL, credentials and all.
LEAKY_TEXT = "Unable to determine SOCKS version from socks9://user:hunter2@proxy.invalid:1080"


def _leaky_chain():
    """An exception shaped exactly the way `check_balance` builds one.

    A redacted message on top of the unredacted original as __cause__ (what
    `raise ... from e` produces). __cause__ is assigned so the object can be handed to
    the harness ready-made; the rendering is identical.
    """
    wrapped = Exception(
        "Error while checking token transactions for address 0xaaa: "
        "ValueError: Unable to determine SOCKS version from socks9://***@proxy.invalid:1080")
    wrapped.__cause__ = ValueError(LEAKY_TEXT)
    return wrapped


def test_the_happy_path_never_logs_the_proxy_with_its_password(monkeypatch):
    # The direct leak, and the worst of the four because it is on the path that
    # always runs: one line per wallet, every round, for as long as the service is
    # up. Nothing has to go wrong for the password to be in `docker logs`.
    wallets = [_Wallet(1, "0xaaa"), _Wallet(2, "0xbbb")]
    harness = _drive_run(monkeypatch, wallets=wallets, iterations=1)
    checked = [message for message in harness.logger.messages if "Check wallet" in message]
    assert len(checked) == 2, harness.logger.messages
    assert not harness.logger.leaking(PROXY_PASSWORD), harness.logger.leaking(PROXY_PASSWORD)
    assert not harness.logger.leaking("user:hunter2@")
    # The proxy is still named: without the host the line stops being able to say
    # WHICH exit the round went out through, and deleting the proxy from it
    # altogether would satisfy a bare "no password" assertion.
    assert all("proxy.invalid" in message for message in checked)


def test_a_failing_round_logs_neither_the_message_nor_the_traceback_unredacted(monkeypatch):
    # The inner handler. Both of its lines carry the exception text and one of them
    # carries the whole formatted traceback, so a leak here is two leaks.
    #
    # Reached through the known-risk `Value`/`Comment` write: it is a Grist call via
    # `requests`, so a ProxyError quoting the proxy URL is the realistic shape. If those
    # columns go, `fail_generate_proxy` is another way in.
    wallets = [_Wallet(1, "0xaaa")]
    harness = _drive_run(monkeypatch, wallets=wallets, iterations=1,
                         fail_check_balance=Exception("balance lookup failed"),
                         fail_update=RuntimeError(
                             "ProxyError: Cannot connect to proxy " + PROXY_URL))
    occurred = [message for message in harness.logger.messages
                if message.startswith("Error occurred:")]
    # Two: the wallet's own handler, whose exception says nothing secret, and the
    # round's, whose exception carries the proxy URL.
    assert len(occurred) == 2, harness.logger.messages
    failed = [message for message in harness.logger.messages if message.startswith("Fail:")]
    assert failed, harness.logger.messages
    assert not harness.logger.leaking(PROXY_PASSWORD), harness.logger.leaking(PROXY_PASSWORD)
    # The redacted message survives, not just a class name. Only the `Fail:` line is
    # cut at the first newline: the traceback appended to it quotes the message again
    # and must not answer for the reason.
    assert PROXY_HOST in occurred[1], occurred[1]
    assert PROXY_HOST in failed[0].split("\n")[0], failed[0]


def test_the_traceback_is_redacted_even_when_the_caught_exception_is_clean(monkeypatch):
    # The case `str(exception)` cannot see and the test above would not catch on its
    # own: the exception reaching the handler says nothing secret, and the secret is
    # in the CHAIN behind it — the unredacted original that `raise ... from e`
    # attaches as __cause__ and that format_exc() prints under "direct cause of".
    # `check_balance` now suppresses its own context with `from None`, but this
    # handler formats whatever chain arrives, and the loop's Grist calls go out
    # through `requests`, which quotes a broken HTTP(S)_PROXY in full.
    wallets = [_Wallet(1, "0xaaa")]
    harness = _drive_run(monkeypatch, wallets=wallets, iterations=1,
                         fail_check_balance=_leaky_chain(),
                         fail_update=True)
    formatted = [message for message in harness.logger.messages if message.startswith("Fail:")]
    assert formatted, harness.logger.messages
    # The traceback really did carry the chain — otherwise this test would pass
    # against a loop that logs no traceback at all.
    assert any("direct cause" in message for message in formatted), formatted
    assert not harness.logger.leaking(PROXY_PASSWORD), harness.logger.leaking(PROXY_PASSWORD)


def test_the_outermost_handler_redacts_too(monkeypatch):
    # The settings fetches sit under this one, and they go out through `requests`
    # inside grist_client: an HTTP(S)_PROXY in the stack's environment arrives here
    # as a GristError quoting the ProxyError, the whole proxy URL included.
    harness = _drive_run(monkeypatch, iterations=1, fail_get_setting=RuntimeError(
        "ProxyError: Cannot connect to proxy " + PROXY_URL))
    lines = [message for message in harness.logger.messages
             if message.startswith("Error occurred, sleep 10s:")]
    assert lines, harness.logger.messages
    assert not harness.logger.leaking(PROXY_PASSWORD), harness.logger.leaking(PROXY_PASSWORD)
    # And the host survives. This line is the ONLY trace such a round leaves — nothing
    # is written to Grist on this path — so a reason reduced to a class name takes the
    # whole diagnosis with it while passing every "no password" assertion above.
    assert PROXY_HOST in lines[0], lines[0]


def test_a_silent_exception_still_produces_a_named_reason(monkeypatch):
    # `requests.exceptions.ConnectionError()` stringifies to nothing, so the old
    # formatting wrote `Error: ` into the wallet's Grist `Comment` column — a cell that
    # is read weeks later, when nobody can tell an empty exception from a redaction that
    # ate the whole message. The class is the one `requests` raises, as in the
    # neighbouring test in tests/test_balances.py, and not the builtin it shares a
    # __name__ with — an assertion that cannot tell them apart is an assertion about
    # nothing.
    wallets = [_Wallet(1, "0xaaa")]
    harness = _drive_run(monkeypatch, wallets=wallets, iterations=1,
                         fail_check_balance=requests.exceptions.ConnectionError())
    comments = [updates["Comment"] for _, updates in harness.grist.updates
                if "Comment" in updates]
    assert comments, harness.grist.updates
    assert all(comment.strip() != "Error:" for comment in comments), comments
    assert all("ConnectionError" in comment for comment in comments), comments
    assert any("ConnectionError" in message for message in harness.logger.messages)


def test_the_round_handler_names_the_class_of_what_it_caught(monkeypatch):
    # Entered through the proxy-preparation switch rather than the known-risk
    # `Value`/`Comment` write, whose column names belong to the Grist document's owner.
    # `requests.exceptions.ConnectionError` (what the loop really catches) stringifies
    # to nothing, so a missing class name shows up as a line ending in its colon.
    harness = _drive_run(monkeypatch, iterations=1,
                         fail_generate_proxy=requests.exceptions.ConnectionError())
    occurred = [message for message in harness.logger.messages
                if message.startswith("Error occurred:")]
    # Exactly one line, matched whole: this handler is the only one that ran, and an
    # equality check is what refuses a reason that is only a colon.
    assert occurred == ["Error occurred: ConnectionError"], harness.logger.messages

    failed = [message for message in harness.logger.messages
              if message.startswith("Fail:")]
    assert failed, harness.logger.messages
    # startswith, NOT `in`: this line carries the formatted traceback, which spells
    # the exception's class by itself. A containment check would pass against a
    # `Fail: ` with an empty reason and a traceback behind it — the exact defect.
    assert failed[0].startswith("Fail: ConnectionError"), failed[0]


def test_the_outermost_handler_names_the_class_of_what_it_caught(monkeypatch):
    # The settings fetches sit under this handler, and grist_client wraps a transport
    # failure there in a GristError. One that carries no message of its own (a bare
    # ConnectionError(), as here) leaves the text at just "Grist request failed: ".
    # Nothing is written to Grist on this path, so this line is the whole record of
    # the round and has to name the class.
    error = GristError("Grist request failed: ")
    error.__cause__ = requests.exceptions.ConnectionError()
    harness = _drive_run(monkeypatch, iterations=1, fail_get_setting=error)
    lines = [message for message in harness.logger.messages
             if message.startswith("Error occurred, sleep 10s:")]
    assert lines, harness.logger.messages
    assert lines[0].startswith("Error occurred, sleep 10s: GristError"), lines


def test_a_wallet_error_reaches_grist_with_the_proxy_credentials_stripped(monkeypatch):
    # The Comment column is written from the caught exception, and a proxy failure
    # carries the proxy URL — password included — in its text. A Grist cell is
    # worse than a log line: people open that document and it goes into backups.
    wallets = [_Wallet(1, "0xaaa")]
    leaky = ("Unable to determine SOCKS version from "
             "socks9://user:hunter2@proxy.invalid:1080")
    harness = _drive_run(monkeypatch, wallets=wallets, iterations=1,
                         fail_check_balance=leaky)
    written = [updates for _, updates in harness.grist.updates if "Comment" in updates]
    assert written, harness.grist.updates
    for updates in written:
        assert "hunter2" not in updates["Comment"]
        assert "user:hunter2@" not in updates["Comment"]
        # The host survives: it is what makes a proxy failure diagnosable.
        assert "proxy.invalid" in updates["Comment"]
