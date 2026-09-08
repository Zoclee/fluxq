# Graph Report - flux  (2026-09-08)

## Corpus Check
- 165 files · ~66,361 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1735 nodes · 5457 edges · 88 communities (45 shown, 43 thin omitted)
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS · INFERRED: 17 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `e9b7d492`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- AmqpPublishConsumeTest
- AmqpConnection
- AmqpTopologyTest
- Application
- AmqpListenerTest
- Flux\Runtime\RuntimeDiagnostics
- BindingRepository
- Connection
- DestinationRepository
- DeliveryRepository
- SchemaTest
- Frame
- Broker
- PublishTransactionTest
- BrokerTopologyManagementTest
- DeliveryRepositoryTest
- UserRepository
- AmqpListener
- composer.json
- BrokerDeliveryTest
- AmqpConnectionTest
- BrokerRuntimeTest
- AdminCommandTest
- virtual_hosts
- AuthenticatedUser
- ServerStartCommand
- AGENTS.md
- VirtualHostRepositoryTest
- UserRepositoryTest
- SubscriptionRepositoryTest
- .writeFrame
- SubscriptionRepository
- TlsCertificate
- MessageRepositoryTest
- MessageRouteRepositoryTest
- ConsumerRegistry
- ResourceLimits
- RuntimeException
- AmqpConnection.php
- Application.php
- ConnectionTest
- ResourcePermissionMatcher
- ConnectionConfig
- BrokerPublishTest
- Dotenv
- DestinationRepositoryTest
- DateTimeImmutable
- BindingRepositoryTest
- AmqpMethodReader
- MessageRepository
- RoutingSourceRepository
- BrokerStatsCommand
- TopicMatcher
- Flux
- ApplicationTest
- PublishResult
- PublishRequestTest
- RoutingSource
- UserClearPermissionsCommand
- ExclusiveQueueRegistry
- QueueShowCommand
- RuntimeDiagnosticsClient
- ConnectionRegistry
- ConsumerRegistryTest
- Changelog
- BrokerRuntimeIntegrationTest
- DbStatusCommand
- Destination
- Flux MVP Smoke Test
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

## Communities (88 total, 43 thin omitted)

### Community 1 - "AmqpConnection"
Cohesion: 0.12
Nodes (3): AmqpConnectionState, Flux\Broker\AuthorizationService, AmqpConnection

### Community 5 - "Flux\Runtime\RuntimeDiagnostics"
Cohesion: 0.05
Nodes (11): Flux\Runtime\RuntimeDiagnostics, ConnectionListCommand, ConsumerListCommand, HealthCommand, ReadinessCommand, AvailableRuntimeDiagnostics, UnavailableRuntimeDiagnostics, ReadinessCommandTest (+3 more)

### Community 7 - "Connection"
Cohesion: 0.12
Nodes (12): DateTimeZone, Flux\Broker\DeliveryState, PDO, PDOException, PHPUnit\Framework\Attributes\Before, PHPUnit\Framework\TestCase, SplFileInfo, VhostCreateCommand (+4 more)

### Community 8 - "DestinationRepository"
Cohesion: 0.07
Nodes (9): Flux\Broker\DestinationType, UserListCommand, UserListPermissionsCommand, UserListVhostsCommand, Table, DestinationRepository, DestinationType, MigrationFailure (+1 more)

### Community 9 - "DeliveryRepository"
Cohesion: 0.16
Nodes (7): Delivery, DeliveryState, DeliveryRepository, DeliveryState, PDO, DeliveryStateException, self

### Community 10 - "SchemaTest"
Cohesion: 0.13
Nodes (3): MigrationResult, PDO, SchemaTest

### Community 11 - "Frame"
Cohesion: 0.10
Nodes (3): Frame, self, FrameCodecTest

### Community 12 - "Broker"
Cohesion: 0.18
Nodes (3): Broker, self, VirtualHostNotFoundException

### Community 16 - "UserRepository"
Cohesion: 0.08
Nodes (7): AuthenticationService, AuthorizationService, Authenticator, Authorizer, UserGrantVhostCommand, UserSetPermissionsCommand, UserRepository

### Community 18 - "composer.json"
Cohesion: 0.08
Nodes (24): autoload, autoload-dev, psr-4, psr-4, bin, config, sort-packages, description (+16 more)

### Community 23 - "virtual_hosts"
Cohesion: 0.11
Nodes (11): virtual_hosts, destinations, bindings, messages, message_routes, subscriptions, deliveries, routing_sources (+3 more)

### Community 24 - "AuthenticatedUser"
Cohesion: 0.09
Nodes (11): Flux\Broker\AuthenticationService, Flux\Protocol\Amqp\AmqpConnectionState, AuthenticatedUser, AuthenticationResult, self, authenticate(), canAccessVirtualHost(), FrameCodec (+3 more)

### Community 26 - "AGENTS.md"
Cohesion: 0.09
Nodes (20): Architecture, Broker, CLI, Console, Core Philosophy, Dependencies, Documentation, graphify (+12 more)

### Community 35 - "ConsumerRegistry"
Cohesion: 0.05
Nodes (10): Closure, RuntimeComponent, RuntimeState, Closure, UserCreateCommand, BrokerRuntime, Closure, ConsumerRegistry (+2 more)

### Community 36 - "ResourceLimits"
Cohesion: 0.11
Nodes (8): Flux\Broker\RoutingSourceType, Closure, self, ResourceLimits, PDO, RoutingSourceType, PublishTransaction, ResourceLimitsTest

### Community 37 - "RuntimeException"
Cohesion: 0.06
Nodes (15): InvalidArgumentException, JsonException, RuntimeException, AcknowledgeRequest, DestinationNotFoundException, self, RejectRequest, ReserveRequest (+7 more)

### Community 38 - "AmqpConnection.php"
Cohesion: 0.09
Nodes (10): Flux\Broker\AuthorizationPermission, Flux\Runtime\RuntimeState, PublishRequest, self, RuntimeConnection, self, RuntimeConsumer, Uuid (+2 more)

### Community 39 - "Application.php"
Cohesion: 0.11
Nodes (6): BindingListCommand, MessagePeekCommand, QueueListCommand, ReadOnlyDatabaseContext, SubscriptionListCommand, VhostListCommand

### Community 41 - "ResourcePermissionMatcher"
Cohesion: 0.12
Nodes (7): AuthorizationResult, self, authorize(), AuthorizationPermission, AuthorizationPermission, ResourcePermissionMatcher, ResourcePermissionMatcherTest

### Community 42 - "ConnectionConfig"
Cohesion: 0.12
Nodes (4): MigrateCommand, self, ConnectionConfig, self

### Community 46 - "DateTimeImmutable"
Cohesion: 0.09
Nodes (8): DateTimeImmutable, MessageRoute, ReleaseRequest, User, UserPermissions, VirtualHost, MessageRouteRepository, RuntimeRegistrationException

### Community 53 - "TopicMatcher"
Cohesion: 0.24
Nodes (3): PHPUnit\Framework\Attributes\DataProvider, TopicMatcher, TopicMatcherTest

### Community 54 - "Flux"
Cohesion: 0.15
Nodes (12): Broker API, CLI, Database Migrations, Directory Structure, Flux, Installation, Known Limitations, MVP Capabilities (+4 more)

### Community 59 - "RoutingSource"
Cohesion: 0.40
Nodes (3): RoutingSourceType, RoutingSourceType, RoutingSource

### Community 66 - "ConnectionRegistry"
Cohesion: 0.12
Nodes (4): Flux\Runtime\RuntimeComponent, Flux\Runtime\RuntimeDrainingComponent, ConnectionRegistry, RecordingRuntimeComponent

### Community 68 - "Changelog"
Cohesion: 0.15
Nodes (12): [0.1.0] - 25 Aug 2026, [0.1.0-RC1] - 24 Aug 2026, [0.1.0-RC2] - 24 Aug 2026, [0.1.0-RC3] - 24 Aug 2026, [0.1.1] - 8 Sep 2026, Added, Changed, Changed (+4 more)

### Community 72 - "Destination"
Cohesion: 0.27
Nodes (3): Destination, DestinationType, QueueStatus

### Community 75 - "Flux MVP Smoke Test"
Cohesion: 0.50
Nodes (3): AMQP Check, Flux MVP Smoke Test, Setup

## Knowledge Gaps
- **53 isolated node(s):** `name`, `description`, `type`, `license`, `flux` (+48 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **43 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Connection` connect `Connection` to `AmqpPublishConsumeTest`, `AmqpTopologyTest`, `Application`, `Flux\Runtime\RuntimeDiagnostics`, `BindingRepository`, `DestinationRepository`, `DeliveryRepository`, `SchemaTest`, `PublishTransactionTest`, `BrokerTopologyManagementTest`, `DeliveryRepositoryTest`, `UserRepository`, `BrokerDeliveryTest`, `AdminCommandTest`, `ServerStartCommand`, `VirtualHostRepositoryTest`, `UserRepositoryTest`, `SubscriptionRepositoryTest`, `SubscriptionRepository`, `MessageRepositoryTest`, `MessageRouteRepositoryTest`, `ConsumerRegistry`, `ResourceLimits`, `AmqpConnection.php`, `Application.php`, `ConnectionTest`, `ConnectionConfig`, `BrokerPublishTest`, `DestinationRepositoryTest`, `DateTimeImmutable`, `BindingRepositoryTest`, `MessageRepository`, `RoutingSourceRepository`, `UserClearPermissionsCommand`, `BrokerRuntimeIntegrationTest`, `DbStatusCommand`?**
  _High betweenness centrality (0.169) - this node is a cross-community bridge._
- **Why does `AmqpPublishConsumeTest` connect `AmqpPublishConsumeTest` to `TlsCertificate`, `Connection`?**
  _High betweenness centrality (0.144) - this node is a cross-community bridge._
- **Why does `Frame` connect `Frame` to `AmqpPublishConsumeTest`, `AmqpConnection`, `AmqpTopologyTest`, `AmqpListenerTest`, `Connection`, `UserRepository`, `.shortString`, `AmqpConnectionTest`, `AuthenticatedUser`, `.sendChannelError`, `.writeFrame`?**
  _High betweenness centrality (0.065) - this node is a cross-community bridge._
- **What connects `name`, `description`, `type` to the rest of the system?**
  _53 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `AmqpPublishConsumeTest` be split into smaller, more focused modules?**
  _Cohesion score 0.08084859052600989 - nodes in this community are weakly interconnected._
- **Should `AmqpConnection` be split into smaller, more focused modules?**
  _Cohesion score 0.11746031746031746 - nodes in this community are weakly interconnected._
- **Should `AmqpTopologyTest` be split into smaller, more focused modules?**
  _Cohesion score 0.14350282485875707 - nodes in this community are weakly interconnected._