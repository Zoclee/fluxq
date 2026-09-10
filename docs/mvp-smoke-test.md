# FluxQ MVP Smoke Test

This is a short manual check for a local MVP release candidate. It assumes PHP, Composer, PostgreSQL, and the PHP `pdo_pgsql` extension are available.

## Setup

```bash
composer install
php fluxq migrate
php fluxq user:create testuser
php fluxq user:grant-vhost testuser /
php fluxq user:set-permissions testuser / ".*" ".*" ".*"
```

`user:create` prompts for the password. Use that same password in the AMQP client connection URL below.

Start FluxQ in one terminal:

```bash
php fluxq server:start
```

From another terminal:

```bash
php fluxq health
php fluxq readiness
php fluxq broker:stats
```

Expected result: health reports `Runtime: healthy`, readiness reports `Ready: yes`, and broker stats reports `Runtime` state `Running`.

## AMQP Check

Use any AMQP 0-9-1 client that supports username/password authentication. Connect to:

```text
amqp://testuser:<password>@127.0.0.1:5672/
```

If TLS is enabled and certificate paths are configured, connect to:

```text
amqps://testuser:<password>@127.0.0.1:5671/
```

Minimal client flow:

```text
1. open connection and channel
2. declare queue "smoke.orders"
3. publish one binary-safe message to the default exchange with routing key "smoke.orders"
4. consume or basic.get from "smoke.orders"
5. acknowledge the delivery
6. optionally enable publisher confirms and verify a basic.ack confirm is received after publish
```

Stop the server with Ctrl+C. FluxQ should enter draining shutdown and exit cleanly.
