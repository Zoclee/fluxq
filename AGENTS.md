# AGENTS.md

This file gives persistent guidance to AI coding agents working on FluxQ.

## Project Context

FluxQ is a unified, high-performance message broker being developed from scratch in vanilla PHP with PostgreSQL.

FluxQ is currently in early development and pre-MVP. Protocol compatibility, broker behavior, database schema, migrations, persistence behavior, and network adapters are not implemented yet.

The current roadmap is:

```text
Phase 1 - PostgreSQL schema, persistence foundation and fluxq CLI
Phase 2 - Protocol-neutral FluxQ broker core
Phase 3 - Minimal AMQP 0-9-1 adapter and functional MVP
Phase 4 - Reliability, observability and production-shaped architecture
```

Respect the current phase. Do not implement future-phase functionality unless explicitly requested.

## Core Philosophy

FluxQ should remain lightweight, fast, explicit, understandable, dependency-conscious, framework-free, and progressively scalable.

Prefer simple, direct PHP implementations over unnecessary abstractions. Do not introduce architecture merely because it is common in large PHP frameworks. Every abstraction must solve a concrete problem in FluxQ.

## PHP

Use modern PHP 8.x. The current project requirement is PHP `^8.4`.

Use `declare(strict_types=1);` for PHP source files where appropriate.

Follow the configured PSR-4 autoloading:

```text
FluxQ\       -> src/
FluxQ\Tests\ -> tests/
```

Prefer explicit types, return types, readonly properties where appropriate, enums where genuinely useful, and clear immutable value objects where they improve correctness. Avoid unnecessary magic behavior.

## Dependencies

FluxQ is intentionally vanilla PHP.

Do not introduce Laravel, Symfony, or another application framework.

Third-party dependencies should be added only when they provide substantial value that would be unreasonable or risky to implement in FluxQ directly.

Do not add:

- ORMs
- dependency-injection frameworks
- application frameworks
- unnecessary abstraction libraries
- CLI frameworks unless the project explicitly decides otherwise

Composer is used for package management, PSR-4 autoloading, testing dependencies, and eventual public distribution. The current development dependency is PHPUnit.

## Architecture

The primary source areas are:

```text
src/
├── Broker/
├── Console/
│   └── Commands/
├── Persistence/
│   └── Postgres/
├── Protocol/
│   └── Amqp/
└── Support/
```

### Broker

Contains the protocol-neutral FluxQ broker core.

The broker must not depend on AMQP, MQTT, Kafka, or other wire protocols.

Protocol adapters and broker-operation CLI commands must call the Broker API rather than persistence repositories directly. The current Broker operation boundary covers publish, reserve, acknowledge, reject, and release. Read-only administrative CLI commands may continue querying persistence repositories directly for inspection.

Runtime connections and runtime consumers are process-memory state only. Do not persist live socket, connection, channel, session, or consumer state. Runtime and protocol operation paths must not bypass the Broker API to perform broker operations through repositories directly. Keep server/runtime behavior cross-platform where practical; Unix-only features must be optional and guarded.

### Console

Contains the `fluxq` CLI implementation and commands.

The CLI should invoke the same underlying FluxQ services as the broker/server. Do not implement duplicate business logic specifically for CLI commands.

### Persistence

Contains persistence-related functionality.

PostgreSQL is the initial storage engine. Broker logic should not become tightly coupled to raw SQL or PostgreSQL-specific implementation details where a clean boundary is practical.

Do not create speculative persistence abstractions merely to support hypothetical databases.

### Protocol

Contains protocol adapters.

Protocol adapters translate between external protocols and FluxQ's internal broker model.

The intended dependency direction is:

```text
AMQP ---\
MQTT ----> FluxQ Broker Core -> Persistence -> PostgreSQL
Kafka --/
```

Protocol adapters must not become the broker core. AMQP concepts must not define FluxQ's internal architecture merely because AMQP is the first implemented protocol.

Protocol adapters should communicate through FluxQ's internal broker operations rather than directly implementing storage behavior.

### Support

Contains only genuinely shared infrastructure. Do not turn `Support` into a dumping ground for unrelated helper classes.

## Unified Broker Principle

FluxQ is not intended to become an AMQP broker with MQTT and Kafka bolted onto it.

FluxQ should have its own protocol-neutral internal broker model. Future cross-protocol flows should remain architecturally possible, such as:

```text
MQTT Producer -> MQTT Adapter -> FluxQ -> AMQP Adapter -> AMQP Consumer
AMQP -> FluxQ -> Kafka
MQTT -> FluxQ -> AMQP
HTTP -> FluxQ -> MQTT
```

Do not assume that concepts from different protocols map one-to-one.

When implementing protocol support, preserve protocol-specific semantics at the adapter boundary while exposing appropriate protocol-neutral operations to the broker core.

## PostgreSQL

PostgreSQL is a foundational component of the initial FluxQ architecture.

Use PDO, prepared statements, transactions, and PostgreSQL capabilities where they provide meaningful advantages.

Do not introduce an ORM.

Database migrations belong under:

```text
database/migrations/
```

Database design must consider concurrency, transactional correctness, message ordering, acknowledgements, crash recovery, and eventual scaling.

Do not make schema changes casually. Database changes should be implemented through migrations.

## CLI

The FluxQ administration CLI is:

```bash
php fluxq <command>
```

The executable entry point is:

```text
fluxq
```

Keep this project-root entry point extremely small. Actual CLI implementation belongs under `src/Console/`. Commands belong under `src/Console/Commands/`.

The CLI is expected to become a major operational and diagnostic interface for FluxQ. It should eventually support inspecting queues, messages, consumers, connections, broker status, health, statistics, and troubleshooting information.

CLI commands must favor safe operational behavior. Destructive operations such as purging or deleting data should require deliberate invocation and appropriate safeguards.

## Testing

Testing is a first-class part of FluxQ development.

Tests are divided into:

```text
tests/
├── Unit/
├── Integration/
└── Fixtures/
```

Unit tests should not require PostgreSQL or network services.

Integration tests may use a real PostgreSQL test database.

Later protocol compatibility tests may use real third-party AMQP, MQTT, and Kafka clients.

When implementing new functionality:

1. Add appropriate tests.
2. Add regression tests for bugs.
3. Test failure paths as well as success paths.
4. Pay particular attention to concurrency and transactional behavior.

Do not weaken or delete existing tests merely to make a change pass.

Run relevant tests before reporting completion. The current test command is:

```bash
composer test
```

## Performance

FluxQ is infrastructure software and performance matters. Correctness comes before premature optimization.

Avoid obviously inefficient designs, especially:

- unnecessary object creation in hot paths
- loading large message sets into memory
- unnecessary database round trips
- polling loops without appropriate blocking or backoff mechanisms
- repeated parsing or serialization
- N+1 database operations

Performance-sensitive changes should eventually be benchmarked rather than assumed to be faster.

## Reliability

Message brokers require stronger correctness guarantees than ordinary CRUD applications.

When changing broker-related functionality, explicitly consider:

- concurrency
- atomicity
- duplicate delivery
- message loss
- ordering
- acknowledgements
- redelivery
- consumer failure
- connection failure
- broker restart
- database failure
- graceful shutdown

Never sacrifice message correctness merely to simplify an implementation.

## Security

Treat all network protocol input as untrusted.

When protocol implementations are introduced:

- validate frame lengths
- validate declared sizes
- enforce configurable limits
- prevent uncontrolled memory allocation
- handle malformed input safely
- avoid unsafe deserialization
- use parameterized SQL
- avoid leaking credentials through logs or errors

Do not commit secrets or environment-specific credentials.

## Scope Discipline

FluxQ will be implemented progressively.

Do not add speculative classes, interfaces, tables, protocols, or abstractions merely because they may eventually be useful.

When given a scoped implementation task:

1. Understand the existing architecture first.
2. Make the smallest coherent architectural change.
3. Preserve existing behavior unless the task requires changing it.
4. Add or update tests.
5. Run relevant tests.
6. Report what changed and what was verified.

If a requested change would conflict with an established architectural principle, flag the conflict rather than silently changing the architecture.

## Documentation

Keep documentation synchronized with meaningful architectural or operational changes.

Important commands, configuration variables, database requirements, and operational behavior should be documented.

Comments should explain why, constraints, invariants, or non-obvious behavior rather than simply restating the code.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, use the installed graphify skill or instructions before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
