# slim is enough: every requirement installs from a prebuilt wheel, and the
# HEALTHCHECK is a heartbeat-file probe with no HTTP (no curl). The 3.9 line is
# end-of-life.
FROM python:3.9-slim

WORKDIR /app

# Unbuffered stdout: `docker logs` is a pipe, and a block-buffered log would lose
# its tail when the container dies mid-round.
ENV PYTHONUNBUFFERED=1

# gosu is used by the entrypoint to drop privileges from root to the app user.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/*

# Fixed uid keeps volume ownership stable across image rebuilds.
RUN useradd -m -u 1000 app

# Dependencies as a separate layer: change less often than code → cached better
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# The service keeps no state (the heartbeat lives in /tmp), but the entrypoint
# chowns data/ on every start under `set -e`, so it has to exist.
RUN mkdir -p data && chown app:app data

# Code only. Deliberately NOT `COPY . .`: tests, CI and the dev requirements stay
# out of the image (see .dockerignore).
COPY src/ src/
COPY main.py ./
# --chmod pins the executable bit: exec-form ENTRYPOINT fails with "permission
# denied" if the bit is lost in the build context (Windows checkout, tar copy).
COPY --chmod=0755 entrypoint.sh /entrypoint.sh

# The probe autoheal acts on. It reads HEARTBEAT_FILE (default
# /tmp/airdrop_checker_heartbeat) and HEARTBEAT_MAX_AGE (default 1200) and exits 0
# only while the main loop's heartbeat is fresh; `-m` makes `src.` imports resolve
# from WORKDIR /app, and `|| exit 1` folds every non-zero status into 1.
# The timings are part of the deploy: after recreating the container on the update
# label, our Portainer build waits for `healthy` within max(120s, start_period +
# 15s) and rolls back otherwise; --start-period=120s lets the loop write its first
# mark. Docker runs the probe outside the ENTRYPOINT, so it drops privileges
# itself (see src/healthcheck.py).
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
  CMD python -m src.healthcheck || exit 1

# No USER directive on purpose: the entrypoint starts as root, heals /app/data
# ownership (migration from older root-based images) and drops to app via gosu.
# A compose `user:` override is respected (the entrypoint then just execs).
ENTRYPOINT ["/entrypoint.sh"]

# Direct invocation with no shell restart loop, so a startup failure kills the
# container visibly; `restart: unless-stopped` restarts it, and the HEALTHCHECK
# catches a hung (rather than dead) loop.
CMD ["python", "main.py"]
