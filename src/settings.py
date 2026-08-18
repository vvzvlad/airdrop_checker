"""Single configuration entry point for the whole service.

Every environment variable the program reads is declared here; nothing else calls
`os.getenv` for configuration. A missing or invalid variable fails at startup with
a message naming it (see `src/config_errors.py`), never with a `None` travelling
into a client.

That last sentence is the defect this file was added for. The old top-level
`airdrop_checker.py` read the three Grist variables with `os.getenv` and handed
them straight to `GristDocAPI` without a single check, so an unconfigured
container died somewhere inside its first HTTP call with a message that named
neither the variable nor the fact that one was missing.

The three variable NAMES below are a production contract: they are what the
`crypt-common` stack on nebula feeds the `airdropchecker` container. Renaming any
of them takes production down.

Note what is NOT here. The loop's own parameters — the proxy string, how many
wallets a round takes, how long to wait between rounds — do not come from the
environment at all: they live in the `Settings` table of the same Grist document
the wallets are in, so they can be changed by the operator without a redeploy.
See `src/checker.py`.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict

from src.config_errors import load_settings_or_exit
from src.heartbeat import DEFAULT_HEARTBEAT_FILE, DEFAULT_HEARTBEAT_MAX_AGE


class Settings(BaseSettings):
    # --- Credentials and self-hosted addresses: fields WITHOUT a default. ------
    # Grist is our own deployment, so its address depends on the deploy and comes
    # from the environment only — a default here would let a misconfigured
    # container talk to the wrong document, or to nothing, without saying so.
    grist_server: str
    grist_doc_id: str
    grist_api_key: str

    # --- Non-secret operational configuration: defaults are fine. -------------
    # Liveness mark consumed by the Docker HEALTHCHECK probe. The default path is
    # part of the production contract (autoheal restarts the container off that
    # probe) and is defined once in src/heartbeat.py.
    heartbeat_file: str = DEFAULT_HEARTBEAT_FILE

    # The probe's freshness window. Declared here so the full configuration
    # surface of the service lives in one object, but the probe itself reads the
    # variable from the environment directly — see the note in src/healthcheck.py
    # for why it must not import this module.
    heartbeat_max_age: int = DEFAULT_HEARTBEAT_MAX_AGE

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Build settings with clear startup errors: a missing/invalid variable prints a
# readable message naming the env var and exits, instead of a raw pydantic
# traceback.
settings = load_settings_or_exit(Settings)
