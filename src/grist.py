"""Thin wrapper around grist_api: column-name sanitising and datetime coercion.

Moved out of the old top-level `airdrop_checker.py` unchanged. Grist accepts a
column by its IDENTIFIER, in which a space is an underscore, while the document's
own labels carry spaces — so every write has to be translated, and that
translation is the whole reason this class exists.

Only the methods the main path actually calls survived the move. The original
class also carried `nodes_table_preprocessing`, `find_record`, `find_chain` and a
commented-out second `find_settings`, none of which was ever called from anywhere
in this repository — they were copy-paste from a sibling service that manages VM
nodes, and they described a table (`State`, `Version`, `Retries`, `Deploy_date`)
that this document does not have. Carrying them here would have implied a schema
that is not there.
"""

from datetime import datetime, timedelta, timezone

from grist_api import GristDocAPI  # type: ignore


class GRIST:
    def __init__(self, server, doc_id, api_key, nodes_table, settings_table, logger):
        self.server = server
        self.doc_id = doc_id
        self.api_key = api_key
        self.nodes_table = nodes_table.replace(" ", "_")
        self.settings_table = settings_table.replace(" ", "_")
        self.logger = logger
        self.grist = GristDocAPI(doc_id, server=server, api_key=api_key)

    def to_timestamp(self, dtime: datetime) -> int:
        # Naive datetimes are read as Moscow time (UTC+3), which is what the
        # Grist document stores. Reading them as UTC instead would silently move
        # every written date three hours, which looks like data rather than like
        # a bug.
        if dtime.tzinfo is None:
            dtime = dtime.replace(tzinfo=timezone(timedelta(hours=3)))
        return int(dtime.timestamp())

    def update_column(self, row_id, column_name, value, table=None):
        if isinstance(value, datetime):
            value = self.to_timestamp(value)
        column_name = column_name.replace(" ", "_")
        self.grist.update_records(table or self.nodes_table, [{"id": row_id, column_name: value}])

    def update(self, row_id, updates, table=None):
        for column_name, value in updates.items():
            if isinstance(value, datetime):
                updates[column_name] = self.to_timestamp(value)
        updates = {column_name.replace(" ", "_"): value for column_name, value in updates.items()}
        self.grist.update_records(table or self.nodes_table, [{"id": row_id, **updates}])

    def fetch_table(self, table=None):
        return self.grist.fetch_table(table or self.nodes_table)

    def find_settings(self, setting, table=None):
        """One row of the `Settings` table, looked up by its `Setting` column.

        Both failures raise instead of returning a default, and that is the point:
        every caller in `src/checker.py` feeds the result straight into `int()` or
        into the proxy string, so a `None` here would come back as an unreadable
        `TypeError` several frames away — or, worse, as a round that quietly ran
        with no proxy at all.
        """
        if table is None:
            table = self.settings_table
        else:
            table = table.replace(" ", "_")
        data = self.grist.fetch_table(table)
        if setting is None:
            raise ValueError("Setting name is not provided")
        if setting not in [row.Setting for row in data]:
            raise ValueError("Setting {} not found in table {}".format(setting, self.settings_table))
        value = [row for row in data if row.Setting == setting][0].Value
        if value == "" or value is None:
            raise ValueError("Setting {} is empty".format(setting))
        return value
