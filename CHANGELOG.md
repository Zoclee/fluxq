# Changelog

All notable changes to FluxQ will be documented in this file.

## [0.2.0] - 10 Sep 2026

### Changed

- Renamed the project from Flux to FluxQ throughout documentation, code, CLI entry points, configuration paths, environment variables, database defaults, tests, and generated project graph metadata.
- Updated the CLI version to `0.2.0`.

## [0.1.1] - 8 Sep 2026

### Changed

- Simplified CLI instructions by moving service .env file to installation folder.
- Added vendor to installation path.

## [0.1.0] - 25 Aug 2026

### Changed

- Added Ubuntu service installation instructions to the README.

## [0.1.0-RC3] - 24 Aug 2026

### Changed

- Cleaned the Composer distribution archive to exclude development-only files and tooling artifacts.

## [0.1.0-RC2] - 24 Aug 2026

### Fixed

- Fixed the `fluxq` CLI bootstrap when FluxQ is installed as a Composer dependency.

## [0.1.0-RC1] - 24 Aug 2026

First release candidate for the FluxQ MVP.

### Added

- PostgreSQL-backed persistence foundation and migrations.
- Protocol-neutral broker core for publish, reserve, acknowledge, reject, and release operations.
- `fluxq` CLI for migrations, health/readiness checks, runtime diagnostics, and administrative inspection.
- AMQP 0-9-1 adapter with queue, exchange, binding, publish, consume, get, ack/reject/nack, QoS, and basic publisher-confirm support.
- AMQP listeners with plaintext and TLS support, username/password authentication, and per-vhost authorization.
- Retry and dead-letter handling, resource limits, overload protection, and graceful bounded shutdown.

### Known Limitations

- This is an MVP release candidate, not a production-hardening release.
- Clustering, replication, management UI, HTTP health endpoints, metrics, MQTT, Kafka, AMQP transactions, and headers exchanges are not included yet.
