<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Broker;

use FluxQ\Broker\Broker;
use FluxQ\Broker\DeliveryState;
use FluxQ\Broker\Destination;
use FluxQ\Broker\PublishRequest;
use FluxQ\Broker\RoutingSourceType;
use FluxQ\Broker\TopologyException;
use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\PublishTransaction;
use FluxQ\Persistence\Postgres\RoutingSourceRepository;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class BrokerTopologyManagementTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private Broker $broker;
    private DestinationRepository $destinations;
    private DeliveryRepository $deliveries;
    private BindingRepository $bindings;
    private RoutingSourceRepository $routingSources;
    private int $virtualHostId;

    #[Before]
    public function setUpBroker(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');
        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL broker integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();
        $this->assertSafeTestDatabase();
        $this->resetSchema();
        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();

        $virtualHosts = new VirtualHostRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $subscriptions = new SubscriptionRepository($this->connection);
        $this->deliveries = new DeliveryRepository($this->connection);
        $this->bindings = new BindingRepository($this->connection);
        $this->routingSources = new RoutingSourceRepository($this->connection);
        $this->broker = new Broker(
            $virtualHosts,
            new PublishTransaction($this->connection),
            $this->destinations,
            $subscriptions,
            $this->deliveries,
            $this->bindings,
            $this->routingSources,
            new MessageRouteRepository($this->connection),
            new MessageRepository($this->connection)
        );
        $this->virtualHostId = $virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testPurgeQueueRejectsOutstandingWorkWithoutDeletingPayload(): void
    {
        $queue = $this->broker->declareQueue('/', 'orders', true, false);
        $this->createDelivery($queue, 'payload');

        $count = $this->broker->purgeQueue('/', 'orders');

        self::assertSame(1, $count);
        self::assertSame(1, $this->deliveryStateCount($queue->id, DeliveryState::Rejected));
        self::assertSame(1, $this->tableCount('messages'));
        self::assertNotNull($this->destinations->findByName($this->virtualHostId, 'orders'));
    }

    public function testDeleteQueueRemovesQueueGraphButLeavesMessagePayloads(): void
    {
        $queue = $this->broker->declareQueue('/', 'orders', true, false);
        $this->createDelivery($queue, 'payload');

        $count = $this->broker->deleteQueue('/', 'orders');

        self::assertSame(1, $count);
        self::assertNull($this->destinations->findByName($this->virtualHostId, 'orders'));
        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(0, $this->tableCount('message_routes'));
        self::assertSame(0, $this->tableCount('deliveries'));
    }

    public function testDeleteQueueIfEmptyFailsTransactionallyForOutstandingWork(): void
    {
        $queue = $this->broker->declareQueue('/', 'orders', true, false);
        $this->createDelivery($queue, 'payload');

        try {
            $this->broker->deleteQueue('/', 'orders', ifEmpty: true);
            self::fail('Non-empty queue deletion should fail.');
        } catch (TopologyException $exception) {
            self::assertSame(TopologyException::PRECONDITION_FAILED, $exception->reason);
            self::assertNotNull($this->destinations->findByName($this->virtualHostId, 'orders'));
            self::assertSame(1, $this->tableCount('deliveries'));
        }
    }

    public function testPassiveQueueDeclarationInspectsExistingQueueWithoutCompatibilityCheck(): void
    {
        $queue = $this->broker->declareQueue('/', 'orders', true, false);

        $passive = $this->broker->declareQueue('/', 'orders', false, true, passive: true);
        $status = $this->broker->queueStatus('/', 'orders');

        self::assertSame($queue->id, $passive->id);
        self::assertSame($queue->id, $status->destination->id);
        self::assertSame(0, $status->messageCount);

        $stored = $this->destinations->findByName($this->virtualHostId, 'orders');
        self::assertNotNull($stored);
        self::assertTrue($stored->durable);
        self::assertFalse($stored->autoDelete);
    }

    public function testPassiveQueueDeclarationOfMissingQueueFailsWithoutCreatingQueue(): void
    {
        try {
            $this->broker->declareQueue('/', 'missing', false, false, passive: true);
            self::fail('Passive queue declaration should fail when the queue does not exist.');
        } catch (TopologyException $exception) {
            self::assertSame(TopologyException::NOT_FOUND, $exception->reason);
        }

        self::assertNull($this->destinations->findByName($this->virtualHostId, 'missing'));
    }

    public function testActiveQueueDeclarationWithEmptyNameGeneratesUniqueQueueName(): void
    {
        $first = $this->broker->declareQueue('/', '', false, false);
        $second = $this->broker->declareQueue('/', '', false, false);

        self::assertStringStartsWith('amq.gen-', $first->name);
        self::assertStringStartsWith('amq.gen-', $second->name);
        self::assertNotSame($first->name, $second->name);
        self::assertNotNull($this->destinations->findByName($this->virtualHostId, $first->name));
        self::assertNotNull($this->destinations->findByName($this->virtualHostId, $second->name));
    }

    public function testPassiveQueueDeclarationWithEmptyNameDoesNotGenerateQueue(): void
    {
        try {
            $this->broker->declareQueue('/', '', false, false, passive: true);
            self::fail('Passive empty queue declaration should fail without generating a queue.');
        } catch (TopologyException $exception) {
            self::assertSame(TopologyException::PRECONDITION_FAILED, $exception->reason);
        }

        self::assertSame(0, $this->destinations->countQueuesByVirtualHost($this->virtualHostId));
    }

    public function testServerNamedQueueGenerationRetriesOnExistingNameCollision(): void
    {
        $this->broker->declareQueue('/', 'amq.gen-collision', false, false);
        $names = ['amq.gen-collision', 'amq.gen-available'];
        $broker = $this->brokerWithServerNamedQueueNames(static function () use (&$names): string {
            return array_shift($names) ?? 'amq.gen-unused';
        });

        $generated = $broker->declareQueue('/', '', false, false);

        self::assertSame('amq.gen-available', $generated->name);
        self::assertNotNull($this->destinations->findByName($this->virtualHostId, 'amq.gen-collision'));
        self::assertNotNull($this->destinations->findByName($this->virtualHostId, 'amq.gen-available'));
    }

    public function testExplicitlyNamedQueueDeclarationRemainsUnchanged(): void
    {
        $queue = $this->broker->declareQueue('/', 'orders', true, false);
        $again = $this->broker->declareQueue('/', 'orders', true, false);

        self::assertSame('orders', $queue->name);
        self::assertSame($queue->id, $again->id);
        self::assertSame(1, $this->destinations->countQueuesByVirtualHost($this->virtualHostId));
    }

    public function testActiveIncompatibleQueueRedeclarationStillFails(): void
    {
        $this->broker->declareQueue('/', 'orders', true, false);

        try {
            $this->broker->declareQueue('/', 'orders', false, false);
            self::fail('Active incompatible queue redeclaration should fail.');
        } catch (TopologyException $exception) {
            self::assertSame(TopologyException::PRECONDITION_FAILED, $exception->reason);
        }
    }

    public function testUnbindAndDeleteRoutingSourceAffectFutureRoutingOnly(): void
    {
        $this->broker->declareQueue('/', 'orders', true, false);
        $this->broker->declareDirectRoutingSource('/', 'orders.direct', true, false);
        $this->broker->bindQueue('/', 'orders.direct', 'orders', 'created');
        $this->broker->unbindQueue('/', 'orders.direct', 'orders', 'created');

        $this->broker->publish(new PublishRequest('/', 'orders.direct', 'created', 'payload'));
        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(0, $this->tableCount('message_routes'));

        $this->broker->deleteRoutingSource('/', 'orders.direct');
        self::assertNull($this->routingSources->findByName($this->virtualHostId, 'orders.direct'));
        self::assertSame(0, $this->bindings->countBySource($this->virtualHostId, 'orders.direct'));
    }

    public function testFanoutRoutingSourceDeclarationRedeclarationAndIncompatibility(): void
    {
        $fanout = $this->broker->declareFanoutRoutingSource('/', 'events', true, false);
        $again = $this->broker->declareFanoutRoutingSource('/', 'events', true, false);

        self::assertSame($fanout->id, $again->id);
        self::assertSame(RoutingSourceType::Fanout, $again->type);

        try {
            $this->broker->declareDirectRoutingSource('/', 'events', true, false);
            self::fail('Redeclaring a fanout routing source as direct should fail.');
        } catch (TopologyException $exception) {
            self::assertSame(TopologyException::PRECONDITION_FAILED, $exception->reason);
        }
    }

    public function testTopicRoutingSourceDeclarationRedeclarationAndIncompatibility(): void
    {
        $topic = $this->broker->declareTopicRoutingSource('/', 'events.topic', true, false);
        $again = $this->broker->declareTopicRoutingSource('/', 'events.topic', true, false);

        self::assertSame($topic->id, $again->id);
        self::assertSame(RoutingSourceType::Topic, $again->type);

        foreach ([
            'direct' => fn () => $this->broker->declareDirectRoutingSource('/', 'events.topic', true, false),
            'fanout' => fn () => $this->broker->declareFanoutRoutingSource('/', 'events.topic', true, false),
        ] as $type => $declare) {
            try {
                $declare();
                self::fail(sprintf('Redeclaring a topic routing source as %s should fail.', $type));
            } catch (TopologyException $exception) {
                self::assertSame(TopologyException::PRECONDITION_FAILED, $exception->reason);
            }
        }
    }

    public function testPassiveRoutingSourceDeclarationInspectsExistingSourceWithoutPropertyCompatibilityCheck(): void
    {
        $source = $this->broker->declareDirectRoutingSource('/', 'events', true, false);

        $passive = $this->broker->declareDirectRoutingSource('/', 'events', false, true, passive: true);
        $status = $this->broker->routingSourceStatus('/', 'events', RoutingSourceType::Direct);

        self::assertSame($source->id, $passive->id);
        self::assertSame($source->id, $status->id);

        $stored = $this->routingSources->findByName($this->virtualHostId, 'events');
        self::assertNotNull($stored);
        self::assertSame(RoutingSourceType::Direct, $stored->type);
        self::assertTrue($stored->durable);
        self::assertFalse($stored->autoDelete);
    }

    public function testPassiveRoutingSourceDeclarationWithWrongTypeStillFails(): void
    {
        $this->broker->declareDirectRoutingSource('/', 'events', true, false);

        try {
            $this->broker->declareFanoutRoutingSource('/', 'events', false, false, passive: true);
            self::fail('Passive routing-source declaration with a different type should fail.');
        } catch (TopologyException $exception) {
            self::assertSame(TopologyException::PRECONDITION_FAILED, $exception->reason);
        }
    }

    public function testPassiveRoutingSourceDeclarationOfMissingSourceFailsWithoutCreatingSource(): void
    {
        try {
            $this->broker->declareDirectRoutingSource('/', 'missing', false, false, passive: true);
            self::fail('Passive routing-source declaration should fail when the source does not exist.');
        } catch (TopologyException $exception) {
            self::assertSame(TopologyException::NOT_FOUND, $exception->reason);
        }

        self::assertNull($this->routingSources->findByName($this->virtualHostId, 'missing'));
    }

    public function testFanoutBindPublishUnbindAndDelete(): void
    {
        $orders = $this->broker->declareQueue('/', 'orders', true, false);
        $audit = $this->broker->declareQueue('/', 'audit', true, false);
        $unbound = $this->broker->declareQueue('/', 'unbound', true, false);
        $this->broker->declareFanoutRoutingSource('/', 'events', true, false);
        $this->broker->bindQueue('/', 'events', 'orders', 'orders-key');
        $this->broker->bindQueue('/', 'events', 'audit', 'audit-key');

        $result = $this->broker->publish(new PublishRequest('/', 'events', 'ignored', 'payload', persistUnrouted: false));

        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(2, $result->routeCount());
        self::assertEqualsCanonicalizing(
            [$orders->id, $audit->id],
            array_map(static fn ($route): int => $route->destinationId, $result->routes)
        );
        self::assertNotContains($unbound->id, array_map(static fn ($route): int => $route->destinationId, $result->routes));

        $this->broker->unbindQueue('/', 'events', 'audit', 'audit-key');
        $afterUnbind = $this->broker->publish(new PublishRequest('/', 'events', 'still-ignored', 'payload', persistUnrouted: false));
        self::assertSame(1, $afterUnbind->routeCount());
        self::assertSame($orders->id, $afterUnbind->routes[0]->destinationId);

        $this->broker->deleteRoutingSource('/', 'events');
        self::assertNull($this->routingSources->findByName($this->virtualHostId, 'events'));
        self::assertSame(0, $this->bindings->countBySource($this->virtualHostId, 'events'));
    }

    public function testTopicBindPublishUnbindAndDelete(): void
    {
        $orders = $this->broker->declareQueue('/', 'orders', true, false);
        $audit = $this->broker->declareQueue('/', 'audit', true, false);
        $this->broker->declareTopicRoutingSource('/', 'events.topic', true, false);
        $this->broker->bindQueue('/', 'events.topic', 'orders', 'orders.*');
        $this->broker->bindQueue('/', 'events.topic', 'audit', '#.failed');

        $result = $this->broker->publish(new PublishRequest('/', 'events.topic', 'orders.failed', 'payload', persistUnrouted: false));

        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(2, $result->routeCount());
        self::assertEqualsCanonicalizing(
            [$orders->id, $audit->id],
            array_map(static fn ($route): int => $route->destinationId, $result->routes)
        );

        $this->broker->unbindQueue('/', 'events.topic', 'audit', '#.failed');
        $afterUnbind = $this->broker->publish(new PublishRequest('/', 'events.topic', 'orders.failed', 'payload', persistUnrouted: false));
        self::assertSame(1, $afterUnbind->routeCount());
        self::assertSame($orders->id, $afterUnbind->routes[0]->destinationId);

        $this->broker->deleteRoutingSource('/', 'events.topic');
        self::assertNull($this->routingSources->findByName($this->virtualHostId, 'events.topic'));
        self::assertSame(0, $this->bindings->countBySource($this->virtualHostId, 'events.topic'));
    }

    private function createDelivery(Destination $destination, string $payload): void
    {
        $message = (new MessageRepository($this->connection))->create($payload);
        $route = (new MessageRouteRepository($this->connection))->create($message->id, $destination->id);
        $subscriptions = new SubscriptionRepository($this->connection);
        $subscription = $subscriptions->findByName($destination->id, 'amqp')
            ?? $subscriptions->create($destination->id, 'amqp');

        $this->deliveries->create($route->id, $subscription->id);
    }

    private function brokerWithServerNamedQueueNames(callable $serverNamedQueueNames): Broker
    {
        return new Broker(
            new VirtualHostRepository($this->connection),
            new PublishTransaction($this->connection),
            $this->destinations,
            new SubscriptionRepository($this->connection),
            $this->deliveries,
            $this->bindings,
            $this->routingSources,
            new MessageRouteRepository($this->connection),
            new MessageRepository($this->connection),
            serverNamedQueueNames: $serverNamedQueueNames
        );
    }

    private function deliveryStateCount(int $destinationId, DeliveryState $state): int
    {
        $statement = $this->pdo->prepare('SELECT COUNT(*) FROM deliveries WHERE destination_id = :destination_id AND state = :state');
        $statement->execute(['destination_id' => $destinationId, 'state' => $state->value]);

        return (int) $statement->fetchColumn();
    }

    private function tableCount(string $table): int
    {
        return (int) $this->pdo->query(sprintf('SELECT count(*) FROM %s', $table))->fetchColumn();
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function assertSafeTestDatabase(): void
    {
        $database = (string) $this->pdo->query('SELECT current_database()')->fetchColumn();

        if (!str_contains(strtolower($database), 'test')) {
            self::markTestSkipped(sprintf(
                'Refusing to reset PostgreSQL database "%s"; FLUXQ_TEST_DATABASE_URL must point to a test database.',
                $database
            ));
        }
    }
}
