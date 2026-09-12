<p align="center">
  <img src="docs/fluxq-logo.svg" alt="FluxQ" width="220">
</p>

FluxQ is a unified message broker built for reliability, extensibility, and interoperability, with a pure PHP core and PostgreSQL persistence.

FluxQ is currently at an MVP release-candidate stopping point. The first supported protocol adapter is AMQP 0-9-1, backed by the protocol-neutral Broker core and PostgreSQL persistence.

## Installation

These steps install FluxQ from source and run it as a `systemd` service on an Ubuntu server. Replace `v0.2.0` with the release tag you want to deploy.

1. Install the required operating-system packages:

```bash
sudo apt update
sudo apt install -y ca-certificates composer git postgresql php-cli php-pgsql unzip
php -v
php -m | grep -E 'openssl|PDO|pdo_pgsql'
```

FluxQ requires PHP 8.4 or newer. If your Ubuntu release does not provide PHP 8.4 packages, install PHP 8.4 from your preferred trusted package source before continuing.

2. Create a dedicated system user and install FluxQ under `/opt/zoclee/fluxq`:

```bash
sudo adduser --system --group --no-create-home --home /opt/zoclee/fluxq fluxq
sudo mkdir -p /opt/zoclee
sudo git clone https://github.com/Zoclee/fluxq.git /opt/zoclee/fluxq
cd /opt/zoclee/fluxq
sudo git checkout v0.2.0
sudo chown -R fluxq:fluxq /opt/zoclee/fluxq
sudo -u fluxq composer install --no-dev --optimize-autoloader
```

3. Create the PostgreSQL role and database:

```bash
sudo -u postgres psql
```

```sql
CREATE ROLE fluxq WITH LOGIN PASSWORD 'change-this-database-password';
CREATE DATABASE fluxq OWNER fluxq;
\q
```

4. Create FluxQ's environment file:

```bash
sudo install -o root -g fluxq -m 0640 /dev/null /opt/zoclee/fluxq/.env
sudoedit /opt/zoclee/fluxq/.env
```

```text
FLUXQ_DB_HOST=127.0.0.1
FLUXQ_DB_PORT=5432
FLUXQ_DB_NAME=fluxq
FLUXQ_DB_USER=fluxq
FLUXQ_DB_PASSWORD=change-this-database-password

FLUXQ_AMQP_ENABLED=true
FLUXQ_AMQP_HOST=0.0.0.0
FLUXQ_AMQP_PORT=5672
FLUXQ_AMQP_HEARTBEAT=60

FLUXQ_DIAGNOSTICS_HOST=127.0.0.1
FLUXQ_DIAGNOSTICS_PORT=5673
```

Keep the diagnostics listener bound to `127.0.0.1`; it is intended for local administrative CLI checks.

5. Apply database migrations:

```bash
cd /opt/zoclee/fluxq
sudo -u fluxq php fluxq db:status
sudo -u fluxq php fluxq migrate
```

6. Create an AMQP user, grant access to the default virtual host, and set permissions:

```bash
cd /opt/zoclee/fluxq
sudo -u fluxq php fluxq user:create app
sudo -u fluxq php fluxq user:grant-vhost app /
sudo -u fluxq php fluxq user:set-permissions app / ".*" ".*" ".*"
```

`user:create` prompts for the AMQP password. Store that password securely and use it in AMQP client connection strings.

7. Install the `systemd` service:

```bash
sudo tee /etc/systemd/system/fluxq.service >/dev/null <<'EOF'
[Unit]
Description=FluxQ message broker
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=fluxq
Group=fluxq
WorkingDirectory=/opt/zoclee/fluxq
EnvironmentFile=/opt/zoclee/fluxq/.env
ExecStart=/usr/bin/php /opt/zoclee/fluxq/fluxq server:start
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=45
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/opt/zoclee/fluxq/var

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now fluxq
```

8. Verify the service:

```bash
systemctl status fluxq
cd /opt/zoclee/fluxq
sudo -u fluxq php fluxq health
sudo -u fluxq php fluxq readiness
```

Expected result: `health` reports `Runtime: healthy`, and `readiness` reports `Ready: yes`.

AMQP clients can connect to:

```text
amqp://app:<password>@<server-host>:5672/
```

## MVP Capabilities

- PostgreSQL-backed persistence
- `fluxq` CLI migrations and administrative diagnostics
- AMQP 0-9-1 connection/channel handshake
- queue declare/delete/purge
- default exchange
- direct exchange declare/delete
- fanout exchange declare/delete
- topic exchange declare/delete
- queue bind/unbind
- `basic.publish`
- `basic.consume`
- `basic.get`
- `basic.ack`, `basic.reject`, and `basic.nack`
- `basic.qos` prefetch-count enforcement
- minimal individual publisher confirms
- heartbeats and disconnect cleanup
- plaintext AMQP listener
- TLS AMQP listener
- username/password authentication
- per-vhost `configure`, `write`, and `read` authorization
- retry and dead-letter handling
- resource limits and overload protection
- graceful bounded draining/shutdown
- local runtime diagnostics plus CLI health/readiness checks

## Known Limitations

- no clustering or replication
- no MQTT or Kafka adapters yet
- no headers exchanges
- no AMQP transactions
- no mandatory returns or alternate exchanges
- no management UI or HTTP health endpoints
- no Prometheus/OpenTelemetry metrics subsystem
- no packaged systemd unit, Docker image, or process-supervisor packaging
- no mutual TLS or certificate-based user authentication
- no rolling restart or zero-downtime upgrade workflow

## Requirements

- PHP 8.4 or newer
- PHP extensions: `pdo`, `pdo_pgsql`, and `openssl`
- Composer
- PostgreSQL for integration tests and persistence work

## CLI

The repository command entry point is `fluxq` at the project root:

```bash
php fluxq
php fluxq help
php fluxq --version
php fluxq db:status
php fluxq migrate
php fluxq health
php fluxq readiness
php fluxq server:start
php fluxq connection:list
php fluxq consumer:list
php fluxq broker:stats
php fluxq user:list-vhosts test_user
php fluxq vhost:create /development
php fluxq vhost:list
php fluxq queue:list
php fluxq queue:show orders
php fluxq binding:list
php fluxq subscription:list
php fluxq message:peek orders
```

The intended Composer-installed command format is:

```bash
fluxq <command>
```

Queue, binding, subscription, and message commands are administrative inspection commands over persisted state.

`php fluxq health` checks whether the local runtime diagnostics endpoint is reachable and currently running. `php fluxq readiness` additionally checks that FluxQ is ready to accept broker traffic, including runtime state, listener status, database connectivity, and migration status.

## Broker API

FluxQ now exposes publishing through the protocol-neutral `FluxQ\Broker\Broker` service. It also has a foreground, long-running protocol-neutral runtime that can host future protocol adapters:

```text
Protocol adapters
        |
   Broker Runtime
        |
      Broker
        |
Persistence orchestration
        |
    PostgreSQL
```

The Broker API accepts broker-facing concepts such as virtual-host name, routing source, routing key, payload bytes, headers, and message metadata. It now owns the protocol-neutral broker operation boundary for `publish`, `reserve`, `acknowledge`, `reject`, and `release`. Future protocol adapters and broker-operation commands should use this boundary rather than constructing PostgreSQL repositories directly.

The runtime can be started with:

```bash
php fluxq server:start
```

It verifies PostgreSQL connectivity, starts the in-memory runtime registries, starts enabled AMQP listeners and local diagnostics, and remains in the foreground until shutdown.

The plaintext AMQP listener defaults to `127.0.0.1:5672` and can be configured with:

```text
FLUXQ_AMQP_ENABLED
FLUXQ_AMQP_HOST
FLUXQ_AMQP_PORT
FLUXQ_AMQP_HEARTBEAT
```

The TLS AMQP listener is disabled by default and can be configured with:

```text
FLUXQ_AMQP_TLS_ENABLED
FLUXQ_AMQP_TLS_HOST
FLUXQ_AMQP_TLS_PORT
FLUXQ_AMQP_TLS_CERT
FLUXQ_AMQP_TLS_KEY
FLUXQ_AMQP_TLS_CA
```

`FLUXQ_AMQP_HEARTBEAT` defaults to `60` seconds. Set it to `0` to disable heartbeat negotiation and timeout cleanup.
Runtime diagnostics are exposed through a small read-only local socket used by `health`, `readiness`, `connection:list`, `consumer:list`, and `broker:stats`. It defaults to `127.0.0.1:5673` and does not expose credentials, message payloads, or mutation commands.

Authentication uses persisted username/password credentials. Users must be granted access to virtual hosts with `php fluxq user:grant-vhost <username> <vhost>`; inspect those grants with `php fluxq user:list-vhosts <username>`. Authorization uses separate persisted per-vhost `configure`, `write`, and `read` regex permissions.

Resource limits and graceful shutdown can be configured with:

```text
FLUXQ_MAX_CONNECTIONS
FLUXQ_MAX_CHANNELS_PER_CONNECTION
FLUXQ_MAX_CONSUMERS_PER_CONNECTION
FLUXQ_MAX_CONSUMERS_PER_CHANNEL
FLUXQ_AMQP_MAX_FRAME_SIZE
FLUXQ_MAX_MESSAGE_SIZE
FLUXQ_MAX_QUEUES_PER_VHOST
FLUXQ_MAX_QUEUE_DEPTH
FLUXQ_SHUTDOWN_DRAIN_TIMEOUT
```

### Database Migrations

FluxQ verifies PostgreSQL connectivity and reports migration status without applying migrations with:

```bash
php fluxq db:status
```

FluxQ applies PostgreSQL migrations with:

```bash
php fluxq migrate
```

After Composer installation, the equivalent command is `fluxq migrate`.

Database configuration is read from normal environment variables:

```text
FLUXQ_DB_HOST
FLUXQ_DB_PORT
FLUXQ_DB_NAME
FLUXQ_DB_USER
FLUXQ_DB_PASSWORD
```

The current defaults are defined in `config/fluxq.php`.

For local development, FluxQ also loads a `.env` file from the project root before reading configuration:

```text
FLUXQ_DB_HOST=127.0.0.1
FLUXQ_DB_PORT=5432
FLUXQ_DB_NAME=fluxq
FLUXQ_DB_USER=fluxq
FLUXQ_DB_PASSWORD=
```

Values already present in the process environment take precedence over `.env`.

## MVP Smoke Test

See `docs/mvp-smoke-test.md` for a short manual smoke-test path that covers installation, migrations, user/vhost permissions, runtime startup, health/readiness, and a minimal AMQP publish/consume flow.

## Tests

```bash
composer test
```

Unit tests live in `tests/Unit/`. Integration tests live in `tests/Integration/` and use a real PostgreSQL test database when `FLUXQ_TEST_DATABASE_URL` is set.

Example:

```bash
FLUXQ_TEST_DATABASE_URL="pgsql:host=127.0.0.1;port=5432;dbname=fluxq_test;user=fluxq;password=secret" composer test
```

## PostgreSQL Persistence Model

The initial schema lives in plain SQL migrations under `database/migrations/`. It establishes the Phase 1 persistence foundation only:

Migration filenames use `yyyymmdd_hhmmss_description.sql` so lexical order is execution order. If multiple migrations are created at the same time, increment the timestamp by one second for each subsequent file. Migrations must be idempotent and safe to apply more than once.

```text
virtual_hosts
    |
    +-- destinations
    |      |
    |      +-- bindings
    |      |
    |      +-- message_routes
    |               |
    |               +-- deliveries
    |
    +-- subscriptions

messages
    |
    +-- message_routes
```

FluxQ uses `destinations` instead of making queues the fundamental abstraction so the core schema stays protocol-neutral. A queue is the first supported destination type, but future protocol adapters should not force MQTT topics, Kafka topics, or AMQP exchanges into queue-specific tables.

`messages` store payload bytes and message metadata once. `message_routes` associate one stored payload with one or more destinations, allowing fan-out without duplicating binary payload data. `deliveries` are separate because reservation, acknowledgement, rejection, retries, and attempts have their own lifecycle independent of message storage.

Live consumers, TCP connections, channels, sockets, and runtime statistics are not persisted. Durable consumption relationships are represented by `subscriptions`; active consumers remain runtime concepts for later broker phases.

## Directory Structure

- `fluxq` - project-root CLI entry point
- `config/` - FluxQ configuration
- `database/migrations/` - PostgreSQL schema migrations
- `src/Broker/` - protocol-neutral broker core
- `src/Console/` - CLI application commands
- `src/Persistence/` - persistence abstractions and implementations
- `src/Persistence/Postgres/` - PostgreSQL persistence implementation
- `src/Protocol/` - protocol adapters
- `src/Protocol/Amqp/` - AMQP 0-9-1 adapter
- `src/Support/` - small shared infrastructure
- `tests/` - unit, integration, and fixture files
- `var/` - runtime logs and process files
