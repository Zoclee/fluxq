<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Broker;

use DateTimeImmutable;
use FluxQ\Broker\AcknowledgeRequest;
use FluxQ\Broker\Broker;
use FluxQ\Broker\DeliveryState;
use FluxQ\Broker\Destination;
use FluxQ\Broker\DestinationNotFoundException;
use FluxQ\Broker\RejectRequest;
use FluxQ\Broker\ReleaseRequest;
use FluxQ\Broker\ReserveRequest;
use FluxQ\Broker\RetryPolicy;
use FluxQ\Broker\SubscriptionNotFoundException;
use FluxQ\Broker\TopologyException;
use FluxQ\Broker\VirtualHostNotFoundException;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DeliveryStateException;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\PublishTransaction;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class BrokerDeliveryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private Broker $broker;
    private DeliveryRepository $deliveries;
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
        $this->deliveries = new DeliveryRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $this->messages = new MessageRepository($this->connection);
        $this->routes = new MessageRouteRepository($this->connection);
        $this->subscriptions = new SubscriptionRepository($this->connection);
        $this->broker = $this->brokerFor($this->connection);
        $this->defaultVirtualHostId = $virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testReserveByNamesClaimsOldestAvailableDeliveryAndStoresRuntimeCorrelation(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a');
        $first = $this->createPendingDelivery($destination, 'first');
        $second = $this->createPendingDelivery($destination, 'second');

        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a', 'tag-a'));

        self::assertNotNull($reserved);
        self::assertSame($first, $reserved->id);
        self::assertNotSame($second, $reserved->id);
        self::assertSame(DeliveryState::Reserved, $reserved->state);
        self::assertSame('consumer-a', $reserved->consumerId);
        self::assertSame('tag-a', $reserved->deliveryTag);
        self::assertNotNull($reserved->reservedAt);
        self::assertSame(1, $reserved->attempts);
    }

    public function testNoPendingDeliveryReturnsNull(): void
    {
        $this->createDestinationAndSubscription('orders', 'worker-a');

        self::assertNull($this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a')));
    }

    public function testUnknownVirtualHostDestinationAndSubscriptionFailCleanly(): void
    {
        $this->expectException(VirtualHostNotFoundException::class);
        $this->expectExceptionMessage('Virtual host "/missing" does not exist.');

        $this->broker->reserve(new ReserveRequest('/missing', 'orders', 'worker-a', 'consumer-a'));
    }

    public function testUnknownDestinationFailsCleanly(): void
    {
        $this->expectException(DestinationNotFoundException::class);
        $this->expectExceptionMessage('Destination "missing" does not exist in virtual host "/".');

        $this->broker->reserve(new ReserveRequest('/', 'missing', 'worker-a', 'consumer-a'));
    }

    public function testUnknownSubscriptionFailsCleanly(): void
    {
        $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        $this->expectException(SubscriptionNotFoundException::class);
        $this->expectExceptionMessage('Subscription "missing" does not exist for destination "orders".');

        $this->broker->reserve(new ReserveRequest('/', 'orders', 'missing', 'consumer-a'));
    }

    public function testFutureDeliveryIsNotReservedEarly(): void
    {
        [$destination, $subscriptionId] = $this->createDestinationAndSubscription('orders', 'worker-a');
        $route = $this->routes->create(
            $this->messages->create('future')->id,
            $destination->id
        );
        $this->deliveries->create(
            $route->id,
            $subscriptionId,
            new DateTimeImmutable('2030-01-01T00:00:00+00:00')
        );

        self::assertNull($this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a')));
    }

    public function testAcknowledgeRejectAndReleaseDelegateStateTransitions(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a');

        $ackDeliveryId = $this->createPendingDelivery($destination, 'ack');
        $reservedForAck = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reservedForAck);
        self::assertSame($ackDeliveryId, $reservedForAck->id);
        $acknowledged = $this->broker->acknowledge(new AcknowledgeRequest($reservedForAck->id));
        self::assertSame(DeliveryState::Acknowledged, $acknowledged->state);
        self::assertNotNull($acknowledged->acknowledgedAt);

        $rejectDeliveryId = $this->createPendingDelivery($destination, 'reject');
        $reservedForReject = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-b'));
        self::assertNotNull($reservedForReject);
        self::assertSame($rejectDeliveryId, $reservedForReject->id);
        $rejected = $this->broker->reject(new RejectRequest($reservedForReject->id));
        self::assertSame(DeliveryState::Rejected, $rejected->state);

        $releaseDeliveryId = $this->createPendingDelivery($destination, 'release');
        $reservedForRelease = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-c', 'tag-c'));
        self::assertNotNull($reservedForRelease);
        self::assertSame($releaseDeliveryId, $reservedForRelease->id);
        $released = $this->broker->release(new ReleaseRequest($reservedForRelease->id));
        self::assertSame(DeliveryState::Pending, $released->state);
        self::assertNull($released->consumerId);
        self::assertNull($released->deliveryTag);
        self::assertNull($released->reservedAt);
        self::assertSame(1, $released->attempts);

        $redelivered = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-d'));
        self::assertNotNull($redelivered);
        self::assertSame($released->id, $redelivered->id);
        self::assertSame(2, $redelivered->attempts);
    }

    public function testDelayedReleaseIsRespected(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a');
        $this->createPendingDelivery($destination, 'delayed');
        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);

        $availableAt = new DateTimeImmutable('2030-01-01T00:00:00+00:00');
        $released = $this->broker->release(new ReleaseRequest($reserved->id, $availableAt));

        self::assertSame(DeliveryState::Pending, $released->state);
        self::assertSame($availableAt->getTimestamp(), $released->availableAt->getTimestamp());
        self::assertNull($this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-b')));
    }

    public function testInvalidStateTransitionsRemainDistinguishable(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a');
        $pendingDeliveryId = $this->createPendingDelivery($destination, 'pending');

        $this->expectException(DeliveryStateException::class);
        $this->expectExceptionMessage(sprintf(
            'Delivery %d cannot transition',
            $pendingDeliveryId
        ));

        $this->broker->acknowledge(new AcknowledgeRequest($pendingDeliveryId));
    }

    public function testBinaryMessageDataIsUnaffectedByDeliveryLifecycle(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a');
        $payload = "abc\x00def\x80\xFE\xFF";
        $message = $this->messages->create($payload);
        $route = $this->routes->create($message->id, $destination->id);
        $this->deliveries->create($route->id, $this->subscriptions->findByName($destination->id, 'worker-a')?->id ?? 0);

        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);
        $this->broker->release(new ReleaseRequest($reserved->id));
        $reservedAgain = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-b'));
        self::assertNotNull($reservedAgain);

        self::assertSame($payload, $this->messages->findById($message->id)?->payload);
    }

    public function testConcurrentReservationsThroughBrokerCannotClaimTheSameDelivery(): void
    {
        $dsn = (string) getenv('FLUXQ_TEST_DATABASE_URL');
        [$destination, $subscriptionId] = $this->createDestinationAndSubscription('orders', 'worker-a');
        $first = $this->createPendingDelivery($destination, 'first');
        $second = $this->createPendingDelivery($destination, 'second');

        $connectionA = Connection::fromDsn($dsn);
        $connectionB = Connection::fromDsn($dsn);
        $pdoA = $connectionA->pdo();
        $brokerB = $this->brokerFor($connectionB);

        $pdoA->beginTransaction();

        try {
            $lock = $pdoA->prepare(<<<'SQL'
SELECT id
FROM deliveries
WHERE subscription_id = :subscription_id
  AND state = 'pending'
  AND available_at <= CURRENT_TIMESTAMP
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 1
SQL);
            $lock->execute(['subscription_id' => $subscriptionId]);
            self::assertSame($first, (int) $lock->fetchColumn());

            $update = $pdoA->prepare(<<<'SQL'
UPDATE deliveries
SET state = 'reserved',
    consumer_id = 'consumer-a',
    reserved_at = CURRENT_TIMESTAMP,
    attempts = attempts + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :id
SQL);
            $update->execute(['id' => $first]);

            $reservedByB = $brokerB->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-b'));

            self::assertNotNull($reservedByB);
            self::assertSame($second, $reservedByB->id);
            self::assertNotSame($first, $reservedByB->id);
            self::assertSame(DeliveryState::Reserved, $reservedByB->state);
            self::assertSame('consumer-b', $reservedByB->consumerId);
        } finally {
            if ($pdoA->inTransaction()) {
                $pdoA->rollBack();
            }
        }
    }

    public function testRetryPolicyReleasesDeliveryWithFutureAvailabilityBeforeMaxAttempts(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a', [
            'retry_policy' => [
                'max_attempts' => 3,
                'retry_delay_seconds' => 60,
                'dead_letter_destination' => 'orders.dlq',
            ],
        ]);
        $this->destinations->create($this->defaultVirtualHostId, 'orders.dlq', 'queue');
        $this->createPendingDelivery($destination, 'retry');
        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);
        $before = new DateTimeImmutable();

        $failed = $this->broker->reject(new RejectRequest($reserved->id));

        self::assertSame(DeliveryState::Pending, $failed->state);
        self::assertSame(1, $failed->attempts);
        self::assertGreaterThan($before->getTimestamp(), $failed->availableAt->getTimestamp());
        self::assertNull($this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-b')));
    }

    public function testRetriedDeliveryCanBeReservedAgainAndAttemptsContinueIncrementing(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a', [
            'retry_policy' => [
                'max_attempts' => 3,
                'retry_delay_seconds' => 0,
                'dead_letter_destination' => 'orders.dlq',
            ],
        ]);
        $this->destinations->create($this->defaultVirtualHostId, 'orders.dlq', 'queue');
        $this->createPendingDelivery($destination, 'retry');
        $first = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($first);
        $this->broker->reject(new RejectRequest($first->id));

        $second = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-b'));

        self::assertNotNull($second);
        self::assertSame($first->id, $second->id);
        self::assertSame(2, $second->attempts);
    }

    public function testMaxAttemptsRoutesMessageToDeadLetterDestinationWithoutDuplicatingPayload(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a', [
            'retry_policy' => [
                'max_attempts' => 1,
                'retry_delay_seconds' => 0,
                'dead_letter_destination' => 'orders.dlq',
            ],
        ]);
        $dlq = $this->destinations->create($this->defaultVirtualHostId, 'orders.dlq', 'queue');
        $dlqSubscription = $this->subscriptions->create($dlq->id, 'dlq-workers');
        $message = $this->messages->create('dead-letter-payload');
        $route = $this->routes->create($message->id, $destination->id);
        $this->deliveries->create($route->id, $this->subscriptions->findByName($destination->id, 'worker-a')?->id ?? 0);
        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);

        $failed = $this->broker->reject(new RejectRequest($reserved->id));

        self::assertSame(DeliveryState::Rejected, $failed->state);
        self::assertSame(1, $this->messages->countAll());
        $routes = $this->routes->allByMessage($message->id);
        self::assertCount(2, $routes);
        $deadLetterRoutes = array_values(array_filter($routes, static fn ($route): bool => $route->destinationId === $dlq->id));
        self::assertCount(1, $deadLetterRoutes);
        $dlqDeliveries = $this->deliveries->allBySubscription($dlqSubscription->id);
        self::assertCount(1, $dlqDeliveries);
        self::assertSame($deadLetterRoutes[0]->id, $dlqDeliveries[0]->messageRouteId);
        self::assertSame(DeliveryState::Pending, $dlqDeliveries[0]->state);
        self::assertSame('dead-letter-payload', $this->messages->findById($message->id)?->payload);
    }

    public function testMissingDeadLetterDestinationFailsWithoutMutatingOriginalDelivery(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a', [
            'retry_policy' => [
                'max_attempts' => 1,
                'retry_delay_seconds' => 0,
                'dead_letter_destination' => 'missing.dlq',
            ],
        ]);
        $deliveryId = $this->createPendingDelivery($destination, 'missing-dlq');
        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);

        try {
            $this->broker->reject(new RejectRequest($reserved->id));
            self::fail('Expected missing dead-letter destination to fail.');
        } catch (TopologyException $exception) {
            self::assertStringContainsString('Dead-letter destination "missing.dlq" does not exist.', $exception->getMessage());
        }

        $delivery = $this->deliveries->findById($deliveryId);
        self::assertNotNull($delivery);
        self::assertSame(DeliveryState::Reserved, $delivery->state);
        self::assertSame(1, $delivery->attempts);
        self::assertSame(1, $this->routes->countByDestination($destination->id));
    }

    public function testDeadLetterTransactionRollbackPreservesOriginalStateOnPersistenceFailure(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a');
        $deliveryId = $this->createPendingDelivery($destination, 'rollback');
        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);

        try {
            $this->deliveries->fail($reserved->id, new RetryPolicy(1, 0, 'missing'), 999999);
            self::fail('Expected invalid dead-letter destination persistence failure.');
        } catch (\RuntimeException) {
        }

        $delivery = $this->deliveries->findById($deliveryId);
        self::assertNotNull($delivery);
        self::assertSame(DeliveryState::Reserved, $delivery->state);
        self::assertSame(1, $this->routes->countByDestination($destination->id));
        self::assertSame(1, $this->messages->countAll());
    }

    public function testBinaryPayloadSurvivesDeadLetterRouting(): void
    {
        [$destination] = $this->createDestinationAndSubscription('binary', 'worker-a', [
            'retry_policy' => [
                'max_attempts' => 1,
                'retry_delay_seconds' => 0,
                'dead_letter_destination' => 'binary.dlq',
            ],
        ]);
        $dlq = $this->destinations->create($this->defaultVirtualHostId, 'binary.dlq', 'queue');
        $dlqSubscription = $this->subscriptions->create($dlq->id, 'dlq-workers');
        $payload = "abc\x00def\xff";
        $message = $this->messages->create($payload);
        $route = $this->routes->create($message->id, $destination->id);
        $this->deliveries->create($route->id, $this->subscriptions->findByName($destination->id, 'worker-a')?->id ?? 0);
        $reserved = $this->broker->reserve(new ReserveRequest('/', 'binary', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);

        $this->broker->reject(new RejectRequest($reserved->id));

        $dlqDelivery = $this->deliveries->allBySubscription($dlqSubscription->id)[0] ?? null;
        self::assertNotNull($dlqDelivery);
        $dlqRoute = $this->routes->findById($dlqDelivery->messageRouteId);
        self::assertNotNull($dlqRoute);
        self::assertSame($message->id, $dlqRoute->messageId);
        self::assertSame($payload, $this->messages->findById($dlqRoute->messageId)?->payload);
    }

    public function testDeadLetterDestinationCreatesDeliveriesForAllSubscriptions(): void
    {
        [$destination] = $this->createDestinationAndSubscription('orders', 'worker-a', [
            'retry_policy' => [
                'max_attempts' => 1,
                'retry_delay_seconds' => 0,
                'dead_letter_destination' => 'orders.dlq',
            ],
        ]);
        $dlq = $this->destinations->create($this->defaultVirtualHostId, 'orders.dlq', 'queue');
        $this->subscriptions->create($dlq->id, 'dlq-a');
        $this->subscriptions->create($dlq->id, 'dlq-b');
        $this->createPendingDelivery($destination, 'multi-dlq');
        $reserved = $this->broker->reserve(new ReserveRequest('/', 'orders', 'worker-a', 'consumer-a'));
        self::assertNotNull($reserved);

        $this->broker->reject(new RejectRequest($reserved->id));

        self::assertCount(1, $this->deliveries->allBySubscription($this->subscriptions->findByName($dlq->id, 'dlq-a')?->id ?? 0));
        self::assertCount(1, $this->deliveries->allBySubscription($this->subscriptions->findByName($dlq->id, 'dlq-b')?->id ?? 0));
    }

    /**
     * @return array{0: Destination, 1: int}
     * @param array<string, mixed> $metadata
     */
    private function createDestinationAndSubscription(string $destinationName, string $subscriptionName, array $metadata = []): array
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, $destinationName, 'queue', metadata: $metadata);
        $subscription = $this->subscriptions->create($destination->id, $subscriptionName);

        return [$destination, $subscription->id];
    }

    private function createPendingDelivery(Destination $destination, string $payload): int
    {
        $message = $this->messages->create(sprintf('%s-%d', $payload, ++$this->sequence));
        $route = $this->routes->create($message->id, $destination->id);
        $subscription = $this->subscriptions->findByName($destination->id, 'worker-a')
            ?? throw new \RuntimeException('Expected test subscription to exist.');

        return $this->deliveries->create($route->id, $subscription->id)->id;
    }

    private function brokerFor(Connection $connection): Broker
    {
        return new Broker(
            new VirtualHostRepository($connection),
            new PublishTransaction($connection),
            new DestinationRepository($connection),
            new SubscriptionRepository($connection),
            new DeliveryRepository($connection)
        );
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
