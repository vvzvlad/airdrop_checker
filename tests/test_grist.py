"""The Grist wrapper, against a recording double instead of a Grist server.

`GristDocAPI` is replaced wholesale, so nothing here opens a socket. What is
tested is the translation layer between this service and that client: the column
names it rewrites and the timestamps it converts. Both are silent failure modes —
an unrewritten "Wait time max" is simply rejected by Grist, and a timestamp read
in the wrong zone looks like data rather than like a bug.
"""

from datetime import datetime, timedelta, timezone

import pytest

import src.grist
from src.grist import GRIST


class FakeGristDocAPI:
    """Records what the real client would have been asked to do."""

    def __init__(self, doc_id, server=None, api_key=None):
        self.doc_id = doc_id
        self.server = server
        self.api_key = api_key
        self.updates = []
        self.tables = {}

    def update_records(self, table, records):
        self.updates.append((table, records))

    def fetch_table(self, table):
        return self.tables.get(table, [])


class Row:
    def __init__(self, **fields):
        self.__dict__.update(fields)


class _NullLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


@pytest.fixture
def grist(monkeypatch):
    monkeypatch.setattr(src.grist, "GristDocAPI", FakeGristDocAPI)
    return GRIST("http://grist.invalid", "doc-1", "key-1", "Wallets", "Settings", _NullLogger())


# --- construction ------------------------------------------------------------

def test_table_names_are_sanitised_at_construction(monkeypatch):
    monkeypatch.setattr(src.grist, "GristDocAPI", FakeGristDocAPI)
    client = GRIST("s", "d", "k", "My Wallets", "Node Settings", _NullLogger())
    assert client.nodes_table == "My_Wallets"
    assert client.settings_table == "Node_Settings"


def test_the_credentials_are_handed_to_the_client_unchanged(grist):
    assert grist.grist.doc_id == "doc-1"
    assert grist.grist.server == "http://grist.invalid"
    assert grist.grist.api_key == "key-1"


# --- writes ------------------------------------------------------------------

def test_update_rewrites_spaces_in_column_names(grist):
    grist.update(7, {"hypercore hype value": 1.5, "hyperevm_hype_value": 2.5})
    table, records = grist.grist.updates[-1]
    assert table == "Wallets"
    assert records == [{"id": 7, "hypercore_hype_value": 1.5, "hyperevm_hype_value": 2.5}]


def test_update_column_rewrites_spaces_in_the_column_name(grist):
    grist.update_column(7, "Some Column", "value")
    assert grist.grist.updates[-1] == ("Wallets", [{"id": 7, "Some_Column": "value"}])


def test_update_targets_the_nodes_table_by_default_and_an_override_when_given(grist):
    grist.update(1, {"A": 1})
    grist.update(2, {"A": 2}, table="Other")
    assert grist.grist.updates[0][0] == "Wallets"
    assert grist.grist.updates[1][0] == "Other"


def test_update_converts_a_datetime_to_a_unix_timestamp(grist):
    moment = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    grist.update(7, {"Checked": moment})
    _, records = grist.grist.updates[-1]
    assert records == [{"id": 7, "Checked": int(moment.timestamp())}]
    assert isinstance(records[0]["Checked"], int)


def test_update_column_converts_a_datetime_to_a_unix_timestamp(grist):
    moment = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    grist.update_column(7, "Checked At", moment)
    assert grist.grist.updates[-1] == ("Wallets", [{"id": 7, "Checked_At": int(moment.timestamp())}])


def test_a_naive_datetime_is_read_as_moscow_time(grist):
    # UTC+3 is what the Grist document stores. Reading naive datetimes as UTC
    # instead would move every written date three hours — a difference that looks
    # like data, not like a bug.
    naive = datetime(2026, 1, 2, 3, 4, 5)
    aware = naive.replace(tzinfo=timezone(timedelta(hours=3)))
    assert grist.to_timestamp(naive) == int(aware.timestamp())


def test_non_datetime_values_travel_untouched(grist):
    grist.update(7, {"Comment": "Error: boom", "Value": "--"})
    _, records = grist.grist.updates[-1]
    assert records == [{"id": 7, "Comment": "Error: boom", "Value": "--"}]


# --- reads -------------------------------------------------------------------

def test_fetch_table_defaults_to_the_nodes_table(grist):
    grist.grist.tables["Wallets"] = [Row(id=1, Address="0xaaa")]
    assert [row.id for row in grist.fetch_table()] == [1]


def test_find_settings_returns_the_value_column_of_the_matching_row(grist):
    grist.grist.tables["Settings"] = [
        Row(Setting="Proxy", Value="http://proxy.invalid"),
        Row(Setting="Walled count max", Value="5"),
    ]
    assert grist.find_settings("Proxy") == "http://proxy.invalid"
    # The document's own typo. It is spelled this way here because it is spelled
    # this way there — the document belongs to someone else.
    assert grist.find_settings("Walled count max") == "5"


def test_find_settings_raises_when_the_setting_is_absent(grist):
    grist.grist.tables["Settings"] = [Row(Setting="Proxy", Value="http://proxy.invalid")]
    with pytest.raises(ValueError) as exc_info:
        grist.find_settings("Wait time max")
    assert "Wait time max" in str(exc_info.value)


@pytest.mark.parametrize("empty", ["", None])
def test_find_settings_raises_when_the_setting_is_empty(grist, empty):
    # Returning "" here would reach `int("")` two lines up the stack, or would put
    # a round on the network with no proxy at all.
    grist.grist.tables["Settings"] = [Row(Setting="Proxy", Value=empty)]
    with pytest.raises(ValueError) as exc_info:
        grist.find_settings("Proxy")
    assert "empty" in str(exc_info.value)


def test_find_settings_raises_when_no_name_is_given(grist):
    grist.grist.tables["Settings"] = []
    with pytest.raises(ValueError):
        grist.find_settings(None)


def test_find_settings_sanitises_an_overridden_table_name(grist):
    grist.grist.tables["Other_Settings"] = [Row(Setting="Proxy", Value="x")]
    assert grist.find_settings("Proxy", table="Other Settings") == "x"
