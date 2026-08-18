"""The configuration object: the variable NAMES, the defaults, and the failure path.

The three required names are a production contract (the crypt-common stack on
nebula supplies exactly those), and the two heartbeat defaults are the contract
the Docker HEALTHCHECK and auto-heal are built on — so both are pinned here
rather than left to be noticed in production.

The startup-failure test is the one that pins the defect this refactor fixed: the
old top-level script read the three variables with `os.getenv` and never checked
them, so an unconfigured container died inside its first Grist call with a
message that named neither the variable nor the problem.

Every `Settings(...)` here passes `_env_file=None`. Without it a developer's own
`.env` in the repository root would be read and could supply the very variable a
test is trying to remove, making these pass for the wrong reason on one machine
and fail on another.
"""

import pytest
from pydantic import ValidationError

from src.config_errors import load_settings_or_exit
from src.heartbeat import DEFAULT_HEARTBEAT_FILE, DEFAULT_HEARTBEAT_MAX_AGE
from src.settings import Settings

REQUIRED_VARS = ("GRIST_SERVER", "GRIST_DOC_ID", "GRIST_API_KEY")


def _fill_required(monkeypatch):
    monkeypatch.setenv("GRIST_SERVER", "http://grist.invalid")
    monkeypatch.setenv("GRIST_DOC_ID", "doc-1")
    monkeypatch.setenv("GRIST_API_KEY", "key-1")


def _clear_optional(monkeypatch):
    for name in ("HEARTBEAT_FILE", "HEARTBEAT_MAX_AGE"):
        monkeypatch.delenv(name, raising=False)


def test_required_variables_map_to_their_env_names(monkeypatch):
    # If a field were renamed, its env var name would change with it and the
    # crypt-common stack would stop configuring the container.
    _fill_required(monkeypatch)
    _clear_optional(monkeypatch)
    s = Settings(_env_file=None)
    assert s.grist_server == "http://grist.invalid"
    assert s.grist_doc_id == "doc-1"
    assert s.grist_api_key == "key-1"


def test_optional_defaults_match_the_documented_contract(monkeypatch):
    _fill_required(monkeypatch)
    _clear_optional(monkeypatch)
    s = Settings(_env_file=None)
    assert s.heartbeat_file == DEFAULT_HEARTBEAT_FILE == "/tmp/airdrop_checker_heartbeat"
    assert s.heartbeat_max_age == DEFAULT_HEARTBEAT_MAX_AGE == 1200


def test_optional_variables_are_read_from_the_environment(monkeypatch):
    _fill_required(monkeypatch)
    monkeypatch.setenv("HEARTBEAT_FILE", "/tmp/other-heartbeat")
    monkeypatch.setenv("HEARTBEAT_MAX_AGE", "77")
    s = Settings(_env_file=None)
    assert s.heartbeat_file == "/tmp/other-heartbeat"
    assert s.heartbeat_max_age == 77          # coerced to int, not left as "77"


@pytest.mark.parametrize("missing", REQUIRED_VARS)
def test_each_required_variable_is_mandatory(monkeypatch, missing):
    # No silent fallback, no empty default: one absent variable must fail.
    _fill_required(monkeypatch)
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("missing", REQUIRED_VARS)
def test_one_missing_variable_exits_1_and_names_that_variable(capsys, monkeypatch, missing):
    # The convention's error path, one variable at a time: exit code 1, a readable
    # message that NAMES the offending variable, and no pydantic traceback. The
    # name is the whole point — "configuration error" without it sends whoever is
    # on the other end of the deploy to read the source.
    _fill_required(monkeypatch)
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        load_settings_or_exit(lambda: Settings(_env_file=None))
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "Missing required variable(s):" in err
    assert missing in err
    assert "Traceback" not in err
    assert "pydantic" not in err.lower()


def test_all_missing_variables_are_named_at_once(capsys, monkeypatch):
    # Reported together rather than one per restart: a container that has to be
    # redeployed three times to learn about three variables is the failure mode
    # this message replaces.
    for name in REQUIRED_VARS:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit):
        load_settings_or_exit(lambda: Settings(_env_file=None))
    err = capsys.readouterr().err
    for name in REQUIRED_VARS:
        assert name in err


def test_non_numeric_heartbeat_max_age_is_rejected(monkeypatch):
    _fill_required(monkeypatch)
    monkeypatch.setenv("HEARTBEAT_MAX_AGE", "soon")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_extra_environment_variables_do_not_break_startup(monkeypatch):
    # extra="ignore": the container's environment carries plenty of unrelated
    # variables (TZ, PATH, ...) and none of them may fail the start.
    _fill_required(monkeypatch)
    monkeypatch.setenv("SOMETHING_UNRELATED", "value")
    assert Settings(_env_file=None).grist_doc_id == "doc-1"
