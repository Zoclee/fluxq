<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Broker\DeliveryState;
use FluxQ\Broker\Destination;
use FluxQ\Broker\ResourceLimitException;
use FluxQ\Broker\ResourceLimits;
use FluxQ\Broker\RoutingSourceType;
use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\PublishTransaction;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class PublishTransactionTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private BindingRepository $bindings;
    private DestinationRepository $destinations;
    private MessageRepository $messages;
    private MessageRouteRepository $routes;
    private SubscriptionRepository $subscriptions;
    private PublishTransaction $publisher;
    private int $defaultVirtualHostId;
    private int $sequence = 0;

    #[Before]
    public function setUpPublisher(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');

        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL repository integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();

        $this->assertSafeTestDatabase();
        $this->resetSchema();
        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();

        $virtualHosts = new VirtualHostRepository($this->connection);
        $this->bindings = new BindingRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $this->messages = new MessageRepository($this->connection);
        $this->routes = new MessageRouteRepository($this->connection);
        $this->subscriptions = new SubscriptionRepository($this->connection);
        $this->publisher = new PublishTransaction($this->connection);
        $this->defaultVirtualHostId = $virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testOneBindingAndOneSubscriptionCreatesOneCompleteGraph(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $subscription = $this->subscriptions->create($destination->id, 'worker-a');

        $result = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'orders',
            'order.created',
            'payload'
        );

        self::assertSame(1, $result->routeCount());
        self::assertSame(1, $result->deliveryCount());
        self::assertSame($destination->id, $result->routes[0]->destinationId);
        self::assertSame($subscription->id, $result->deliveries[0]->subscriptionId);
        self::assertSame(DeliveryState::Pending, $result->deliveries[0]->state);
        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(1, $this->tableCount('message_routes'));
        self::assertSame(1, $this->tableCount('deliveries'));
    }

    public function testOneDestinationWithMultipleSubscriptionsCreatesOneRouteAndSeveralDeliveries(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $this->subscriptions->create($destination->id, 'worker-a');
        $this->subscriptions->create($destination->id, 'worker-b');
        $this->subscriptions->create($destination->id, 'worker-c');

        $result = $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'payload');

        self::assertSame(1, $result->routeCount());
        self::assertSame(3, $result->deliveryCount());
        self::assertSame(1, $this->tableCount('message_routes'));
        self::assertSame(3, $this->tableCount('deliveries'));
        self::assertCount(1, array_unique(array_map(
            static fn ($delivery): int => $delivery->messageRouteId,
            $result->deliveries
        )));
    }

    public function testMultipleDestinationsFanOutFromOneStoredPayload(): void
    {
        $destinationA = $this->bindDestination('orders', 'order.created', 'orders-a');
        $destinationB = $this->bindDestination('orders', 'order.created', 'orders-b');
        $this->subscriptions->create($destinationA->id, 'worker-a1');
        $this->subscriptions->create($destinationA->id, 'worker-a2');
        $this->subscriptions->create($destinationB->id, 'worker-b1');

        $payload = "\x00\x01\x80\xFFpayload";
        $headers = ['event' => 'created', 'attempt' => 1, 'nested' => ['ok' => true]];
        $result = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'orders',
            'order.created',
            $payload,
            $headers,
            'application/octet-stream',
            'identity',
            7,
            false
        );

        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(2, $result->routeCount());
        self::assertSame(3, $result->deliveryCount());
        self::assertSame(2, $this->tableCount('message_routes'));
        self::assertSame(3, $this->tableCount('deliveries'));

        $message = $this->messages->findById($result->message->id);
        self::assertNotNull($message);
        self::assertSame($payload, $message->payload);
        self::assertEquals($headers, $message->headers);
        self::assertSame('application/octet-stream', $message->contentType);
        self::assertSame('identity', $message->contentEncoding);
        self::assertSame(7, $message->priority);
        self::assertFalse($message->persistent);
    }

    public function testNoMatchingBindingStillCommitsUnroutedMessageByDefault(): void
    {
        $result = $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'missing', 'payload');

        self::assertSame(0, $result->routeCount());
        self::assertSame(0, $result->deliveryCount());
        self::assertSame(1, $this->tableCount('messages'));
        self::assertNotNull($result->message);
        self::assertSame('payload', $result->message->payload);
        self::assertSame(0, $this->tableCount('message_routes'));
        self::assertSame(0, $this->tableCount('deliveries'));
    }

    public function testNoMatchingBindingCanDiscardUnroutedMessageWithoutPersistingGraph(): void
    {
        $result = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'orders',
            'missing',
            'payload',
            persistUnrouted: false
        );

        self::assertNull($result->message);
        self::assertNull($result->messageId());
        self::assertSame(0, $result->routeCount());
        self::assertSame(0, $result->deliveryCount());
        self::assertSame(0, $this->tableCount('messages'));
        self::assertSame(0, $this->tableCount('message_routes'));
        self::assertSame(0, $this->tableCount('deliveries'));
    }

    public function testRoutedDestinationWithoutSubscriptionsCreatesRouteOnly(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');

        $result = $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'payload');

        self::assertSame(1, $result->routeCount());
        self::assertSame(0, $result->deliveryCount());
        self::assertSame($destination->id, $result->routes[0]->destinationId);
        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(1, $this->tableCount('message_routes'));
        self::assertSame(0, $this->tableCount('deliveries'));
    }

    public function testQueueDepthLimitAllowsExactlyAtLimitAndRejectsNextPublish(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $this->subscriptions->create($destination->id, 'worker-a');
        $publisher = new PublishTransaction($this->connection, new ResourceLimits(maxQueueDepth: 1));

        $result = $publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'first');
        self::assertSame(1, $result->deliveryCount());
        $before = $this->graphCounts();

        try {
            $publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'second');
            self::fail('Queue depth limit should reject a publish beyond capacity.');
        } catch (ResourceLimitException $exception) {
            self::assertStringContainsString('Queue depth limit reached', $exception->getMessage());
            self::assertSame($before, $this->graphCounts());
        }
    }

    public function testAcknowledgedDeliveryFreesQueueDepthCapacity(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $subscription = $this->subscriptions->create($destination->id, 'worker-a');
        $publisher = new PublishTransaction($this->connection, new ResourceLimits(maxQueueDepth: 1));
        $result = $publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'first');
        $deliveries = new DeliveryRepository($this->connection);
        $reserved = $deliveries->reserveNext($subscription->id, 'consumer-a', '1');
        self::assertNotNull($reserved);

        try {
            $publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'blocked');
            self::fail('Reserved deliveries should count toward queue depth.');
        } catch (ResourceLimitException) {
        }

        $deliveries->acknowledge($result->deliveries[0]->id);
        $next = $publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'second');

        self::assertSame(1, $next->deliveryCount());
        self::assertSame(2, $this->tableCount('messages'));
        self::assertSame(2, $this->tableCount('deliveries'));
    }

    public function testQueueDepthLimitKeepsFanOutPublishAtomic(): void
    {
        $destinationA = $this->bindDestination('orders', 'order.created', 'orders-a');
        $destinationB = $this->bindDestination('orders', 'order.created', 'orders-b');
        $this->subscriptions->create($destinationA->id, 'worker-a');
        $this->subscriptions->create($destinationB->id, 'worker-b');
        $this->publisher->publishToDestination($destinationB->id, 'already-full');
        $before = $this->graphCounts();
        $publisher = new PublishTransaction($this->connection, new ResourceLimits(maxQueueDepth: 1));

        try {
            $publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'payload');
            self::fail('A full fan-out destination should reject the whole publish.');
        } catch (ResourceLimitException) {
            self::assertSame($before, $this->graphCounts());
        }
    }

    public function testMultipleBindingsResolveToMultipleDestinations(): void
    {
        $destinationA = $this->bindDestination('orders', 'order.created', 'orders-a');
        $destinationB = $this->bindDestination('orders', 'order.created', 'orders-b');
        $this->bindDestination('orders', 'order.cancelled', 'orders-cancelled');
        $this->subscriptions->create($destinationA->id, 'worker-a');
        $this->subscriptions->create($destinationB->id, 'worker-b');

        $result = $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'payload');

        self::assertSame(2, $result->routeCount());
        self::assertSame(2, $result->deliveryCount());
        self::assertEqualsCanonicalizing(
            [$destinationA->id, $destinationB->id],
            array_map(static fn ($route): int => $route->destinationId, $result->routes)
        );
    }

    public function testFanoutIgnoresRoutingKeyAndRoutesEveryBoundDestinationOnce(): void
    {
        $destinationA = $this->bindDestination('events', 'created', 'events-a');
        $destinationB = $this->bindDestination('events', 'updated', 'events-b');
        $this->bindDestination('events.other', 'ignored', 'events-c');
        $this->subscriptions->create($destinationA->id, 'worker-a');
        $this->subscriptions->create($destinationB->id, 'worker-b');
        $this->bindings->create($this->defaultVirtualHostId, 'events', $destinationA->id, 'duplicate-key');

        $result = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'events',
            'not-used',
            'payload',
            persistUnrouted: false,
            sourceType: RoutingSourceType::Fanout
        );

        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(2, $result->routeCount());
        self::assertSame(2, $result->deliveryCount());
        self::assertEqualsCanonicalizing(
            [$destinationA->id, $destinationB->id],
            array_map(static fn ($route): int => $route->destinationId, $result->routes)
        );
    }

    public function testTopicRoutesLiteralStarHashAndMixedPatterns(): void
    {
        $literal = $this->bindDestination('events.topic', 'orders.eu.payment.created', 'literal');
        $star = $this->bindDestination('events.topic', 'orders.*.*.created', 'star');
        $hash = $this->bindDestination('events.topic', 'orders.#', 'hash');
        $hashSuffix = $this->bindDestination('events.topic', '#.created', 'hash-suffix');
        $mixed = $this->bindDestination('events.topic', 'orders.*.#.created', 'mixed');
        $this->bindDestination('events.topic', 'orders.*', 'non-matching-star');
        $this->bindDestination('events.topic', 'audit.#', 'non-matching-hash');

        $result = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'events.topic',
            'orders.eu.payment.created',
            'payload',
            persistUnrouted: false,
            sourceType: RoutingSourceType::Topic
        );

        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(5, $result->routeCount());
        self::assertEqualsCanonicalizing(
            [$literal->id, $star->id, $hash->id, $hashSuffix->id, $mixed->id],
            array_map(static fn ($route): int => $route->destinationId, $result->routes)
        );
    }

    public function testTopicHashMatchesZeroOneAndMultipleWords(): void
    {
        $destination = $this->bindDestination('events.topic', 'stock.#', 'stock');

        foreach (['stock', 'stock.us', 'stock.us.nyse'] as $routingKey) {
            $result = $this->publisher->publish(
                $this->defaultVirtualHostId,
                'events.topic',
                $routingKey,
                'payload',
                persistUnrouted: false,
                sourceType: RoutingSourceType::Topic
            );

            self::assertSame(1, $result->routeCount());
            self::assertSame($destination->id, $result->routes[0]->destinationId);
        }
    }

    public function testTopicStarMatchesExactlyOneWord(): void
    {
        $destination = $this->bindDestination('events.topic', 'stock.*', 'stock-region');

        $oneWord = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'events.topic',
            'stock.us',
            'payload',
            persistUnrouted: false,
            sourceType: RoutingSourceType::Topic
        );
        $zeroWords = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'events.topic',
            'stock',
            'payload',
            persistUnrouted: false,
            sourceType: RoutingSourceType::Topic
        );
        $multipleWords = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'events.topic',
            'stock.us.nyse',
            'payload',
            persistUnrouted: false,
            sourceType: RoutingSourceType::Topic
        );

        self::assertSame(1, $oneWord->routeCount());
        self::assertSame($destination->id, $oneWord->routes[0]->destinationId);
        self::assertSame(0, $zeroWords->routeCount());
        self::assertSame(0, $multipleWords->routeCount());
    }

    public function testTopicDeduplicatesDestinationWhenSeveralBindingsMatch(): void
    {
        $destination = $this->bindDestination('events.topic', 'orders.*', 'orders');
        $this->bindings->create($this->defaultVirtualHostId, 'events.topic', $destination->id, 'orders.#');
        $this->bindings->create($this->defaultVirtualHostId, 'events.topic', $destination->id, '#.created');
        $this->subscriptions->create($destination->id, 'worker-a');

        $result = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'events.topic',
            'orders.created',
            'payload',
            persistUnrouted: false,
            sourceType: RoutingSourceType::Topic
        );

        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(1, $result->routeCount());
        self::assertSame(1, $result->deliveryCount());
        self::assertSame($destination->id, $result->routes[0]->destinationId);
    }

    public function testTopicQueueDepthLimitKeepsMultiDestinationPublishAtomic(): void
    {
        $orders = $this->bindDestination('events.topic', 'orders.*', 'orders');
        $audit = $this->bindDestination('events.topic', '#.failed', 'audit');
        $this->subscriptions->create($orders->id, 'worker-orders');
        $this->subscriptions->create($audit->id, 'worker-audit');
        $this->publisher->publishToDestination($audit->id, 'already-full');
        $before = $this->graphCounts();
        $publisher = new PublishTransaction($this->connection, new ResourceLimits(maxQueueDepth: 1));

        try {
            $publisher->publish(
                $this->defaultVirtualHostId,
                'events.topic',
                'orders.failed',
                'payload',
                persistUnrouted: false,
                sourceType: RoutingSourceType::Topic
            );
            self::fail('A full topic destination should reject the whole publish.');
        } catch (ResourceLimitException) {
            self::assertSame($before, $this->graphCounts());
        }
    }

    public function testBindingUniquenessPreventsDuplicateRouteInputs(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');

        $this->expectException(PDOException::class);

        $this->bindings->create($this->defaultVirtualHostId, 'orders', $destination->id, 'order.created');
    }

    public function testRouteAndDeliveryUniquenessRemainAuthoritative(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $subscription = $this->subscriptions->create($destination->id, 'worker-a');
        $result = $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'payload');

        try {
            $this->routes->create($result->message->id, $destination->id);
            self::fail('Duplicate message route should fail.');
        } catch (PDOException) {
            self::assertSame(1, $this->tableCount('message_routes'));
        }

        try {
            (new DeliveryRepository($this->connection))->create($result->routes[0]->id, $subscription->id);
            self::fail('Duplicate delivery should fail.');
        } catch (PDOException) {
            self::assertSame(1, $this->tableCount('deliveries'));
        }
    }

    public function testExplicitMessageUuidWorksAndPublishResultMatchesPersistedGraph(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $this->subscriptions->create($destination->id, 'worker-a');
        $messageId = '00000000-0000-4000-8000-000000000123';

        $result = $this->publisher->publish(
            $this->defaultVirtualHostId,
            'orders',
            'order.created',
            'payload',
            messageId: $messageId
        );

        self::assertSame($messageId, $result->messageId());
        self::assertSame(1, $result->routeCount());
        self::assertSame(1, $result->deliveryCount());
        self::assertNotNull($this->messages->findByMessageId($messageId));
        self::assertSame($result->message->id, $this->routes->findById($result->routes[0]->id)?->messageId);
    }

    public function testDuplicateExplicitMessageUuidFailsWithoutPartialGraph(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $this->subscriptions->create($destination->id, 'worker-a');
        $messageId = '00000000-0000-4000-8000-000000000124';
        $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'first', messageId: $messageId);

        $before = $this->graphCounts();

        try {
            $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'second', messageId: $messageId);
            self::fail('Duplicate message UUID should fail.');
        } catch (PDOException) {
            self::assertSame($before, $this->graphCounts());
        }
    }

    public function testLateDeliveryFailureRollsBackMessageRouteAndDeliveryGraph(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $this->subscriptions->create($destination->id, 'worker-a');
        $before = $this->graphCounts();
        $this->installDeliveryFailureTrigger();

        try {
            $this->publisher->publish($this->defaultVirtualHostId, 'orders', 'order.created', 'payload');
            self::fail('Delivery trigger should fail the publish transaction.');
        } catch (PDOException) {
            self::assertSame($before, $this->graphCounts());
            self::assertSame(0, $this->tableCount('messages'));
            self::assertSame(0, $this->tableCount('message_routes'));
            self::assertSame(0, $this->tableCount('deliveries'));
        }
    }

    private function bindDestination(string $source, string $routingKey, ?string $name = null): Destination
    {
        $destination = $this->destinations->create(
            $this->defaultVirtualHostId,
            $name ?? sprintf('destination-%d', ++$this->sequence),
            'queue'
        );
        $this->bindings->create($this->defaultVirtualHostId, $source, $destination->id, $routingKey);

        return $destination;
    }

    /**
     * @return array{messages: int, message_routes: int, deliveries: int}
     */
    private function graphCounts(): array
    {
        return [
            'messages' => $this->tableCount('messages'),
            'message_routes' => $this->tableCount('message_routes'),
            'deliveries' => $this->tableCount('deliveries'),
        ];
    }

    private function tableCount(string $table): int
    {
        return (int) $this->pdo->query(sprintf('SELECT count(*) FROM %s', $table))->fetchColumn();
    }

    private function installDeliveryFailureTrigger(): void
    {
        $this->pdo->exec(<<<'SQL'
CREATE OR REPLACE FUNCTION fluxq_test_fail_delivery_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'forced delivery failure for publish rollback test';
END;
$$;

CREATE TRIGGER fluxq_test_fail_delivery_insert
BEFORE INSERT ON deliveries
FOR EACH ROW
EXECUTE FUNCTION fluxq_test_fail_delivery_insert();
SQL);
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
