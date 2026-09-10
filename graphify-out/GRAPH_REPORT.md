# Graph Report - fluxq  (2026-09-10)

## Corpus Check
- 165 files · ~66,402 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1737 nodes · 5459 edges · 85 communities (47 shown, 38 thin omitted)
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS · INFERRED: 17 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `4f94dc33`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- AmqpPublishConsumeTest
- AmqpConnection
- Frame
- Application
- AmqpListener
- FluxQ\Runtime\RuntimeDiagnostics
- BindingRepository
- PHPUnit\Framework\TestCase
- DestinationRepository
- DeliveryRepository
- SchemaTest
- Broker
- PublishTransactionTest
- BrokerTopologyManagementTest
- DeliveryRepositoryTest
- UserRepository
- Connection
- composer.json
- BrokerDeliveryTest
- AmqpConnectionTest
- BrokerRuntimeTest
- AdminCommandTest
- virtual_hosts
- AuthenticatedUser
- RuntimeDiagnosticsServer
- AGENTS.md
- VirtualHostRepositoryTest
- UserRepositoryTest
- SubscriptionRepositoryTest
- .writeFrame
- SubscriptionRepository
- ResourcePermissionMatcher
- MessageRepositoryTest
- MessageRouteRepositoryTest
- ConsumerRegistry
- ResourceLimits
- RuntimeException
- ConnectionRegistry
- Application.php
- ConnectionTest
- AuthorizationResult
- ConnectionConfig
- BrokerPublishTest
- Dotenv
- DestinationRepositoryTest
- DateTimeImmutable
- BindingRepositoryTest
- AmqpMethodReader
- MessageRepository
- RoutingSourceRepository
- DiagnosticsCommandTest
- TopicMatcher
- FluxQ
- ApplicationTest
- UserPermissions
- PublishRequestTest
- ReadinessCommandTest
- ExclusiveQueueRegistry
- Authorizer
- UserCreateCommand
- RuntimeDiagnosticsClient
- VhostCreateCommand
- ConsumerRegistryTest
- Changelog
- FluxQ MVP Smoke Test
- ConnectionRegistryTest
- 20260820_120000_create_schema_migrations.sql

## God Nodes (most connected - your core abstractions)
1. `AmqpPublishConsumeTest` - 190 edges
2. `Frame` - 161 edges
3. `Connection` - 137 edges
4. `AmqpConnection` - 116 edges
5. `Broker` - 74 edges
6. `AmqpTopologyTest` - 63 edges
7. `AmqpListener` - 54 edges
8. `DestinationRepository` - 49 edges
9. `ConnectionConfig` - 48 edges
10. `DeliveryRepository` - 45 edges

## Surprising Connections (you probably didn't know these)
- `BrokerDeliveryTest` --references--> `Broker`  [EXTRACTED]
  tests/Integration/Broker/BrokerDeliveryTest.php → src/Broker/Broker.php
- `BrokerPublishTest` --references--> `Broker`  [EXTRACTED]
  tests/Integration/Broker/BrokerPublishTest.php → src/Broker/Broker.php
- `BrokerTopologyManagementTest` --references--> `Broker`  [EXTRACTED]
  tests/Integration/Broker/BrokerTopologyManagementTest.php → src/Broker/Broker.php
- `AdminCommandTest` --references--> `ReadOnlyDatabaseContext`  [EXTRACTED]
  tests/Integration/Postgres/AdminCommandTest.php → src/Console/Commands/ReadOnlyDatabaseContext.php
- `BrokerPublishTest` --references--> `BindingRepository`  [EXTRACTED]
  tests/Integration/Broker/BrokerPublishTest.php → src/Persistence/Postgres/BindingRepository.php

## Import Cycles
- None detected.

## Communities (85 total, 38 thin omitted)

### Community 2 - "Frame"
Cohesion: 0.10
Nodes (5): Frame, self, ProtocolException, AmqpTopologyTest, FrameCodecTest

### Community 4 - "AmqpListener"
Cohesion: 0.06
Nodes (5): AmqpListener, AmqpTlsConfig, TlsCertificate, AmqpListenerTest, BrokerRuntimeIntegrationTest

### Community 5 - "FluxQ\Runtime\RuntimeDiagnostics"
Cohesion: 0.07
Nodes (9): FluxQ\Runtime\RuntimeDiagnostics, ConnectionListCommand, ConsumerListCommand, HealthCommand, ReadinessCommand, AvailableRuntimeDiagnostics, UnavailableRuntimeDiagnostics, ReadyRuntimeDiagnostics (+1 more)

### Community 7 - "PHPUnit\Framework\TestCase"
Cohesion: 0.13
Nodes (10): DateTimeZone, FluxQ\Broker\DeliveryState, PDO, PDOException, PHPUnit\Framework\Attributes\Before, PHPUnit\Framework\TestCase, SplFileInfo, Migrator (+2 more)

### Community 8 - "DestinationRepository"
Cohesion: 0.09
Nodes (9): FluxQ\Broker\DestinationType, Destination, DestinationType, QueueStatus, self, RetryPolicy, DestinationRepository, DestinationType (+1 more)

### Community 9 - "DeliveryRepository"
Cohesion: 0.16
Nodes (7): Delivery, DeliveryState, DeliveryRepository, DeliveryState, PDO, DeliveryStateException, self

### Community 10 - "SchemaTest"
Cohesion: 0.13
Nodes (3): MigrationResult, PDO, SchemaTest

### Community 12 - "Broker"
Cohesion: 0.13
Nodes (6): Broker, RoutingSourceType, RoutingSourceType, RoutingSource, self, VirtualHostNotFoundException

### Community 16 - "UserRepository"
Cohesion: 0.14
Nodes (4): AuthenticationService, Authenticator, User, UserRepository

### Community 17 - "Connection"
Cohesion: 0.07
Nodes (9): UserClearPermissionsCommand, UserGrantVhostCommand, UserListCommand, UserListPermissionsCommand, UserListVhostsCommand, UserSetPermissionsCommand, Table, Connection (+1 more)

### Community 18 - "composer.json"
Cohesion: 0.08
Nodes (24): autoload, autoload-dev, psr-4, psr-4, bin, config, sort-packages, description (+16 more)

### Community 23 - "virtual_hosts"
Cohesion: 0.11
Nodes (11): virtual_hosts, destinations, bindings, messages, message_routes, subscriptions, deliveries, routing_sources (+3 more)

### Community 24 - "AuthenticatedUser"
Cohesion: 0.10
Nodes (10): FluxQ\Broker\AuthenticationService, FluxQ\Protocol\Amqp\AmqpConnectionState, AuthenticatedUser, AuthenticationResult, self, authenticate(), canAccessVirtualHost(), FrameCodec (+2 more)

### Community 25 - "RuntimeDiagnosticsServer"
Cohesion: 0.16
Nodes (3): RuntimeComponent, RuntimeDiagnosticsServer, RuntimeDiagnosticsServerTest

### Community 26 - "AGENTS.md"
Cohesion: 0.09
Nodes (20): Architecture, Broker, CLI, Console, Core Philosophy, Dependencies, Documentation, graphify (+12 more)

### Community 35 - "ConsumerRegistry"
Cohesion: 0.10
Nodes (5): RuntimeState, ServerStartCommand, BrokerRuntime, Closure, ConsumerRegistry

### Community 36 - "ResourceLimits"
Cohesion: 0.08
Nodes (9): Closure, PublishResult, ResourceLimitException, self, ResourceLimits, PDO, RoutingSourceType, PublishTransaction (+1 more)

### Community 37 - "RuntimeException"
Cohesion: 0.06
Nodes (13): InvalidArgumentException, JsonException, RuntimeException, AcknowledgeRequest, DestinationNotFoundException, self, RejectRequest, ReserveRequest (+5 more)

### Community 38 - "ConnectionRegistry"
Cohesion: 0.05
Nodes (16): FluxQ\Broker\AuthorizationPermission, FluxQ\Broker\AuthorizationService, FluxQ\Runtime\RuntimeComponent, FluxQ\Runtime\RuntimeDrainingComponent, FluxQ\Runtime\RuntimeState, PublishRequest, ConnectionRegistry, self (+8 more)

### Community 39 - "Application.php"
Cohesion: 0.07
Nodes (10): BindingListCommand, BrokerStatsCommand, DeliveryState, MessagePeekCommand, QueueListCommand, DeliveryState, QueueShowCommand, ReadOnlyDatabaseContext (+2 more)

### Community 41 - "AuthorizationResult"
Cohesion: 0.25
Nodes (4): AuthorizationResult, self, authorize(), AuthorizationPermission

### Community 42 - "ConnectionConfig"
Cohesion: 0.12
Nodes (4): DbStatusCommand, MigrateCommand, ConnectionConfig, self

### Community 46 - "DateTimeImmutable"
Cohesion: 0.11
Nodes (5): DateTimeImmutable, MessageRoute, ReleaseRequest, VirtualHost, MessageRouteRepository

### Community 50 - "RoutingSourceRepository"
Cohesion: 0.16
Nodes (4): Closure, FluxQ\Broker\RoutingSourceType, RoutingSourceType, RoutingSourceRepository

### Community 53 - "TopicMatcher"
Cohesion: 0.24
Nodes (3): PHPUnit\Framework\Attributes\DataProvider, TopicMatcher, TopicMatcherTest

### Community 54 - "FluxQ"
Cohesion: 0.15
Nodes (12): Broker API, CLI, Database Migrations, Directory Structure, FluxQ, Installation, Known Limitations, MVP Capabilities (+4 more)

### Community 62 - "Authorizer"
Cohesion: 0.33
Nodes (3): AuthorizationService, Authorizer, AuthorizationPermission

### Community 68 - "Changelog"
Cohesion: 0.13
Nodes (14): [0.1.0] - 25 Aug 2026, [0.1.0-RC1] - 24 Aug 2026, [0.1.0-RC2] - 24 Aug 2026, [0.1.0-RC3] - 24 Aug 2026, [0.1.1] - 8 Sep 2026, [0.2.0] - 10 Sep 2026, Added, Changed (+6 more)

### Community 75 - "FluxQ MVP Smoke Test"
Cohesion: 0.50
Nodes (3): AMQP Check, FluxQ MVP Smoke Test, Setup

## Knowledge Gaps
- **54 isolated node(s):** `name`, `description`, `type`, `license`, `fluxq` (+49 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **38 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Connection` connect `Connection` to `AmqpPublishConsumeTest`, `Frame`, `Application`, `AmqpListener`, `FluxQ\Runtime\RuntimeDiagnostics`, `BindingRepository`, `PHPUnit\Framework\TestCase`, `DestinationRepository`, `DeliveryRepository`, `SchemaTest`, `PublishTransactionTest`, `BrokerTopologyManagementTest`, `DeliveryRepositoryTest`, `UserRepository`, `BrokerDeliveryTest`, `AdminCommandTest`, `VirtualHostRepositoryTest`, `UserRepositoryTest`, `SubscriptionRepositoryTest`, `SubscriptionRepository`, `MessageRepositoryTest`, `MessageRouteRepositoryTest`, `ConsumerRegistry`, `ResourceLimits`, `ConnectionRegistry`, `Application.php`, `ConnectionTest`, `ConnectionConfig`, `BrokerPublishTest`, `DestinationRepositoryTest`, `DateTimeImmutable`, `BindingRepositoryTest`, `MessageRepository`, `RoutingSourceRepository`, `ReadinessCommandTest`, `UserCreateCommand`, `VhostCreateCommand`?**
  _High betweenness centrality (0.168) - this node is a cross-community bridge._
- **Why does `AmqpPublishConsumeTest` connect `AmqpPublishConsumeTest` to `Connection`, `AmqpListener`, `PHPUnit\Framework\TestCase`?**
  _High betweenness centrality (0.144) - this node is a cross-community bridge._
- **Why does `Frame` connect `Frame` to `AmqpPublishConsumeTest`, `AmqpConnection`, `AmqpListener`, `PHPUnit\Framework\TestCase`, `.handleBasicCancel`, `.shortString`, `AmqpConnectionTest`, `AuthenticatedUser`, `.sendChannelError`, `.writeFrame`?**
  _High betweenness centrality (0.065) - this node is a cross-community bridge._
- **What connects `name`, `description`, `type` to the rest of the system?**
  _54 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `AmqpPublishConsumeTest` be split into smaller, more focused modules?**
  _Cohesion score 0.08084859052600989 - nodes in this community are weakly interconnected._
- **Should `AmqpConnection` be split into smaller, more focused modules?**
  _Cohesion score 0.1106612685560054 - nodes in this community are weakly interconnected._
- **Should `Frame` be split into smaller, more focused modules?**
  _Cohesion score 0.10126582278481013 - nodes in this community are weakly interconnected._