# ── Dev stage: interactive development with all tools ──
FROM ubuntu:22.04 AS dev

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    python3 \
    netcat-openbsd \
    git \
    strace \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dev stage stops here — source is mounted via docker-compose volume.
# Usage: docker compose run --rm gateway-dev bash

# ── Build stage: compile inside the container (for CI) ──
FROM dev AS build

COPY . .
RUN mkdir -p build && cd build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release \
    && make -j$(nproc)

# ── Dashboard stage: Node/Express observability UI ──
FROM node:20-bookworm-slim AS dashboard-dev

WORKDIR /app/dashboard

COPY dashboard/package*.json ./
RUN npm ci

COPY dashboard/ ./

EXPOSE 3000

CMD ["npm", "start"]

# ── Runtime stage: minimal image for deployment ──
FROM ubuntu:22.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/build/gateway /usr/local/bin/gateway
COPY --from=build /app/config/ /etc/gateway/

ENTRYPOINT ["gateway"]
CMD ["--config", "/etc/gateway/gateway.yaml"]
