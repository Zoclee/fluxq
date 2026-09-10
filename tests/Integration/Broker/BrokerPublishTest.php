<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Broker;

use FluxQ\Broker\Broker;
use FluxQ\Broker\DeliveryState;
use FluxQ\Broker\Destination;
use FluxQ\Broker\PublishRequest;
use FluxQ\Broker\VirtualHostNotFoundException;
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

final class BrokerPublishTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private Broker $broker;
    private BindingRepository $bindings;
    private DestinationRepository $destinations;
    private MessageRepository $messages;
    private MessageRouteRepository $routes;
    private SubscriptionRepository $subscriptions;
    private int $defaultVirtualHostId;
    private int $sequence = 0;

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
        $this->bindings = new BindingRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $this->messages = new MessageRepository($this->connection);
        $this->routes = new MessageRouteRepository($this->connection);
        $this->subscriptions = new SubscriptionRepository($this->connection);
        $this->broker = new Broker(
            $virtualHosts,
            new PublishTransaction($this->connection),
            $this->destinations,
            $this->subscriptions,
            new DeliveryRepository($this->connection)
        );
        $this->defaultVirtualHostId = $virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testPublishIntoExistingVirtualHostStoresMessageAndReturnsUuid(): void
    {
        $result = $this->broker->publish(new PublishRequest('/', 'orders', 'order.created', 'payload'));

        self::assertGreaterThan(0, $result->message->id);
        self::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $result->messageId()
        );
        self::assertSame(1, $this->tableCount('messages'));
    }

    public function testUnknownVirtualHostFailsCleanlyWithoutPersistingMessage(): void
    {
        try {
            $this->broker->publish(new PublishRequest('/missing', 'orders', 'order.created', 'payload'));
            self::fail('Publishing to an unknown virtual host should fail.');
        } catch (VirtualHostNotFoundException $exception) {
            self::assertSame('Virtual host "/missing" does not exist.', $exception->getMessage());
            self::assertSame(0, $this->tableCount('messages'));
        }
    }

    public function testExplicitUuidAndMetadataArePersistedThroughBroker(): void
    {
        $messageId = '00000000-0000-4000-8000-000000000321';
        $headers = [
            'event' => 'created',
            'attempt' => 1,
            'nested' => ['ok' => true, 'tags' => ['a', 'b']],
        ];

        $result = $this->broker->publish(new PublishRequest(
            virtualHost: '/',
            source: 'orders',
            routingKey: 'order.created',
            payload: 'metadata',
            headers: $headers,
            contentType: 'application/json',
            contentEncoding: 'identity',
            priority: 9,
            persistent: false,
            messageId: $messageId
        ));

        $message = $this->messages->findByMessageId($messageId);

        self::assertSame($messageId, $result->messageId());
        self::assertNotNull($message);
        self::assertSame($result->message->id, $message->id);
        self::assertEquals($headers, $message->headers);
        self::assertSame('application/json', $message->contentType);
        self::assertSame('identity', $message->contentEncoding);
        self::assertSame(9, $message->priority);
        self::assertFalse($message->persistent);
    }

    public function testBinaryPayloadsArePreservedThroughBroker(): void
    {
        foreach (["\x00\x01\x7F\x80\xFE\xFF", "abc\x00def"] as $payload) {
            $result = $this->broker->publish(new PublishRequest('/', 'binary', 'bytes', $payload));
            $message = $this->messages->findById($result->message->id);

            self::assertNotNull($message);
            self::assertSame($payload, $result->message->payload);
            self::assertSame($payload, $message->payload);
        }
    }

    public function testNoMatchingBindingStillCommitsUnroutedMessageByDefault(): void
    {
        $result = $this->broker->publish(new PublishRequest('/', 'orders', 'missing', 'payload'));

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
        $result = $this->broker->publish(new PublishRequest(
            '/',
            'orders',
            'missing',
            'payload',
            persistUnrouted: false
        ));

        self::assertNull($result->message);
        self::assertNull($result->messageId());
        self::assertSame(0, $result->routeCount());
        self::assertSame(0, $result->deliveryCount());
        self::assertSame(0, $this->tableCount('messages'));
        self::assertSame(0, $this->tableCount('message_routes'));
        self::assertSame(0, $this->tableCount('deliveries'));
    }

    public function testOneBindingCreatesOneRouteThroughBroker(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');

        $result = $this->broker->publish(new PublishRequest('/', 'orders', 'order.created', 'payload'));

        self::assertSame(1, $result->routeCount());
        self::assertSame(0, $result->deliveryCount());
        self::assertSame($destination->id, $result->routes[0]->destinationId);
        self::assertSame($result->message->id, $this->routes->findById($result->routes[0]->id)?->messageId);
    }

    public function testOneDestinationWithMultipleSubscriptionsCreatesMultipleDeliveries(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $subscriptionA = $this->subscriptions->create($destination->id, 'worker-a');
        $subscriptionB = $this->subscriptions->create($destination->id, 'worker-b');

        $result = $this->broker->publish(new PublishRequest('/', 'orders', 'order.created', 'payload'));

        self::assertSame(1, $result->routeCount());
        self::assertSame(2, $result->deliveryCount());
        self::assertEqualsCanonicalizing(
            [$subscriptionA->id, $subscriptionB->id],
            array_map(static fn ($delivery): int => $delivery->subscriptionId, $result->deliveries)
        );
        self::assertCount(1, array_unique(array_map(
            static fn ($delivery): int => $delivery->messageRouteId,
            $result->deliveries
        )));
        self::assertSame(DeliveryState::Pending, $result->deliveries[0]->state);
    }

    public function testMultipleDestinationsFanOutFromOneStoredPayloadThroughBroker(): void
    {
        $destinationA = $this->bindDestination('orders', 'order.created', 'orders-a');
        $destinationB = $this->bindDestination('orders', 'order.created', 'orders-b');
        $this->subscriptions->create($destinationA->id, 'worker-a1');
        $this->subscriptions->create($destinationA->id, 'worker-a2');
        $this->subscriptions->create($destinationB->id, 'worker-b1');
        $payload = "\x00\x01\x80\xFFpayload";

        $result = $this->broker->publish(new PublishRequest('/', 'orders', 'order.created', $payload));

        self::assertSame(1, $this->tableCount('messages'));
        self::assertSame(2, $result->routeCount());
        self::assertSame(3, $result->deliveryCount());
        self::assertSame(2, $this->tableCount('message_routes'));
        self::assertSame(3, $this->tableCount('deliveries'));
        self::assertEqualsCanonicalizing(
            [$destinationA->id, $destinationB->id],
            array_map(static fn ($route): int => $route->destinationId, $result->routes)
        );
        self::assertSame($payload, $this->messages->findById($result->message->id)?->payload);
    }

    public function testBrokerPublishRollbackPreservesGraphOnPersistenceFailure(): void
    {
        $destination = $this->bindDestination('orders', 'order.created');
        $this->subscriptions->create($destination->id, 'worker-a');
        $before = $this->graphCounts();
        $this->installDeliveryFailureTrigger();

        try {
            $this->broker->publish(new PublishRequest('/', 'orders', 'order.created', 'payload'));
            self::fail('Delivery trigger should fail the broker publish transaction.');
        } catch (PDOException) {
            self::assertSame($before, $this->graphCounts());
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
    RAISE EXCEPTION 'forced delivery failure for broker publish rollback test';
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
