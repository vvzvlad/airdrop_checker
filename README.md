# airdrop_checker

A single-process loop. It reads the `Wallets` table of a Grist document, picks wallets
that have an address but no HYPE values yet, asks three purrfolio.com endpoints how much
HYPE each one holds (through a rotating proxy), writes the two numbers back, and sleeps.

The loop's own parameters — the proxy string, how many wallets per round, how long to
sleep — are **not** environment variables: they live in the `Settings` table of the same
Grist document, so the operator changes them without a redeploy.

That is the whole service. It serves no HTTP, exposes no port and keeps no state of its
own.

## Quick start

```bash
make install           # create .venv and install dev/test deps
make env               # cp .env.example .env
$EDITOR .env           # fill in the three required variables
make test              # run the suite
make run               # start the loop
```

`make help` lists every target.

## Configuration

Everything comes from the environment (or `.env` locally) through `src/settings.py`.
See `.env.example` for the annotated list.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `GRIST_SERVER` | yes | — | Grist server URL |
| `GRIST_DOC_ID` | yes | — | Grist document id holding the `Wallets` / `Settings` tables |
| `GRIST_API_KEY` | yes | — | Grist API key |
| `HEARTBEAT_FILE` | no | `/tmp/airdrop_checker_heartbeat` | Liveness mark written each iteration |
| `HEARTBEAT_MAX_AGE` | no | `1200` | Age in seconds above which the HEALTHCHECK probe reports unhealthy |

A missing or invalid variable prints a message naming it and exits 1 — it does not start.

`HEARTBEAT_FILE` must be overridden in the environment, never in `.env`: the loop reads it
through `src/settings.py` (which loads `.env`), while the probe reads the environment only.
Set it in `.env` alone and the loop marks one path while the probe watches another, so the
container stays `unhealthy` forever.

## Liveness

The loop touches `HEARTBEAT_FILE` on every iteration. The image's `HEALTHCHECK` runs
`python -m src.healthcheck`, which exits 0 while that file is fresher than
`HEARTBEAT_MAX_AGE` and non-zero when it is missing or stale. In production the container
carries `io.portainer.autoheal.enable`, so a hung loop gets restarted.

## Deployment

CI (Gitea Actions) runs the tests, builds the image, smoke-tests it inside a container and
pushes `gitea.vvzvlad.xyz/projects/airdrop_checker:<sha>` followed by `:latest`.
Production is the `airdropchecker` service of the `crypt-common` stack on nebula;
`docker-compose.yml` in this repo is the standalone reference for that service.
