# python:3.9-slim. The 3.9 line is end-of-life and moving to a supported one is
# worth doing, but that is a separate decision with its own testing and is not
# carried here — this change is only about the variant.
#
# The full image put this at 1.12 GB, of which the service is 25 MB (21 MB of
# wheels, 4 MB of gosu, 44 KB of source). The rest is base layers, and a single
# 656 MB one is the build toolchain (gcc, make, *-dev) that the official image
# needs once to compile CPython and that is dead weight at runtime. Nothing here
# compiles — every requirement installs from a prebuilt wheel.
# No curl is needed either: the HEALTHCHECK below runs `python -m src.healthcheck`,
# a heartbeat-file probe with no HTTP in it.
FROM python:3.9-slim

WORKDIR /app

# Unbuffered stdout: without it python block-buffers when its output is a pipe,
# which is what `docker logs` is — so the log of a container that dies mid-round
# loses whatever was still in the buffer, in exactly the run worth reading.
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

# Runtime state directory. This service keeps no state of its own today (the
# heartbeat lives in /tmp, by contract), but the directory has to exist and be
# owned by app: the entrypoint chowns it on every start and `set -e` would kill
# the container if it were missing.
RUN mkdir -p data && chown app:app data

# Code only. Deliberately NOT `COPY . .`: tests, CI and the dev requirements stay
# out of the image (see .dockerignore).
COPY src/ src/
COPY main.py ./
# --chmod pins the executable bit: exec-form ENTRYPOINT fails with "permission
# denied" if the bit is lost in the build context (Windows checkout, tar copy).
COPY --chmod=0755 entrypoint.sh /entrypoint.sh

# The probe autoheal acts on — this image had NO healthcheck at all before, which
# made the container's `io.portainer.autoheal.enable` label a string nothing ever
# read. It reads HEARTBEAT_FILE (default /tmp/airdrop_checker_heartbeat) and
# HEARTBEAT_MAX_AGE (default 1200) and exits 0 only while the main loop's
# heartbeat is fresh; `-m` is what makes `from src.heartbeat import ...` resolve
# from WORKDIR /app, and the `|| exit 1` folds every non-zero status into the 1
# docker wants.
# The timings are part of the deploy mechanism rather than cosmetics: after our
# Portainer build recreates this container on the update label it waits for
# `healthy` within max(120s, start_period + 15s) and rolls the image back if the
# window closes first. --start-period=120s is what gives the loop time to write
# its first mark before a probe can score it.
# Docker runs this OUTSIDE the ENTRYPOINT, so the gosu drop below never applies to
# it and the probe would otherwise run as root every 60 s while the service runs as
# `app`. It drops privileges itself instead (see src/healthcheck.py), which keeps
# the drop in one place and lets the test suite exercise both branches of it.
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
  CMD python -m src.healthcheck || exit 1

# No USER directive on purpose: the entrypoint starts as root, heals /app/data
# ownership (migration from older root-based images) and drops to app via gosu.
# A compose `user:` override is respected (the entrypoint then just execs).
ENTRYPOINT ["/entrypoint.sh"]

# A DIRECT invocation, not the old `while true; do python airdrop_checker.py;
# sleep 10; done`. That loop made every startup failure invisible: a container
# with broken configuration looked alive and simply restarted itself forever,
# with nothing above the shell ever reporting a non-zero exit. The process is
# meant to fail loudly instead — production runs this with
# `restart: unless-stopped`, which is what does the restarting, and the
# HEALTHCHECK above is what makes a hung (rather than dead) loop visible.
CMD ["python", "main.py"]
