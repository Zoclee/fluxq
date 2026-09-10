<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use DateTimeImmutable;
use DateTimeZone;
use FluxQ\Broker\Delivery;
use FluxQ\Broker\DeliveryState;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DeliveryStateException;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class DeliveryRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private MessageRepository $messages;
    private DestinationRepository $destinations;
    private MessageRouteRepository $routes;
    private SubscriptionRepository $subscriptions;
    private DeliveryRepository $deliveries;
    private int $defaultVirtualHostId;
    private int $sequence = 0;

    #[Before]
    public function setUpRepository(): void
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
        $this->messages = new MessageRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $this->routes = new MessageRouteRepository($this->connection);
        $this->subscriptions = new SubscriptionRepository($this->connection);
        $this->deliveries = new DeliveryRepository($this->connection);
        $this->defaultVirtualHostId = $virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testDeliveryCanBeCreatedWithInitialDefaults(): void
    {
        [$routeId, $subscriptionId, $destinationId] = $this->createRouteAndSubscription('created');

        $delivery = $this->deliveries->create($routeId, $subscriptionId);

        self::assertGreaterThan(0, $delivery->id);
        self::assertSame($routeId, $delivery->messageRouteId);
        self::assertSame($subscriptionId, $delivery->subscriptionId);
        self::assertSame($destinationId, $delivery->destinationId);
        self::assertSame(DeliveryState::Pending, $delivery->state);
        self::assertSame(0, $delivery->attempts);
        self::assertNull($delivery->consumerId);
        self::assertNull($delivery->deliveryTag);
        self::assertNull($delivery->reservedAt);
        self::assertNull($delivery->acknowledgedAt);
        self::assertNotSame('', $delivery->availableAt->format(DATE_ATOM));
        self::assertNotSame('', $delivery->createdAt->format(DATE_ATOM));
        self::assertNotSame('', $delivery->updatedAt->format(DATE_ATOM));
    }

    public function testExplicitFutureAvailabilityRoundTrips(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('future');
        $availableAt = new DateTimeImmutable('2030-01-01T12:13:14.123456+02:00');

        $delivery = $this->deliveries->create($routeId, $subscriptionId, $availableAt);
        $found = $this->deliveries->findById($delivery->id);

        self::assertNotNull($found);
        self::assertSameTimestamp($availableAt, $found->availableAt);
    }

    public function testFindByIdWorksAndUnknownIdReturnsNull(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('lookup');
        $created = $this->deliveries->create($routeId, $subscriptionId);

        $found = $this->deliveries->findById($created->id);

        self::assertNotNull($found);
        self::assertSame($created->id, $found->id);
        self::assertSame($routeId, $found->messageRouteId);
        self::assertNull($this->deliveries->findById(999999));
    }

    public function testAllBySubscriptionScopesAndOrdersById(): void
    {
        [$firstRouteId, $firstSubscriptionId] = $this->createRouteAndSubscription('scope-a');
        $secondRouteId = $this->createRouteForExistingSubscriptionDestination($firstSubscriptionId);
        [, $otherSubscriptionId] = $this->createRouteAndSubscription('scope-b');

        $first = $this->deliveries->create($firstRouteId, $firstSubscriptionId);
        $other = $this->deliveries->create($this->createRouteForExistingSubscriptionDestination($otherSubscriptionId), $otherSubscriptionId);
        $second = $this->deliveries->create($secondRouteId, $firstSubscriptionId);

        $deliveries = $this->deliveries->allBySubscription($firstSubscriptionId);

        self::assertSame([$first->id, $second->id], array_map(
            static fn (Delivery $delivery): int => $delivery->id,
            $deliveries
        ));
        self::assertNotContains($other->id, array_map(static fn (Delivery $delivery): int => $delivery->id, $deliveries));
    }

    public function testForeignKeysAndDestinationConsistencyAreEnforced(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('fk');

        $this->expectException(PDOException::class);
        $this->deliveries->create(999999, $subscriptionId);
    }

    public function testUnknownSubscriptionFails(): void
    {
        [$routeId] = $this->createRouteAndSubscription('unknown-subscription');

        $this->expectException(PDOException::class);
        $this->deliveries->create($routeId, 999999);
    }

    public function testRouteSubscriptionDestinationMismatchFails(): void
    {
        [$routeId] = $this->createRouteAndSubscription('route-destination');
        [, $subscriptionId] = $this->createRouteAndSubscription('subscription-destination');

        $this->expectException(PDOException::class);
        $this->deliveries->create($routeId, $subscriptionId);
    }

    public function testDuplicateDeliveryForRouteSubscriptionPairFails(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('duplicate');

        $this->deliveries->create($routeId, $subscriptionId);

        $this->expectException(PDOException::class);
        $this->deliveries->create($routeId, $subscriptionId);
    }

    public function testReserveNextClaimsOldestAvailablePendingDelivery(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('reserve-oldest');
        $secondRouteId = $this->createRouteForExistingSubscriptionDestination($subscriptionId);
        $first = $this->deliveries->create($routeId, $subscriptionId);
        $second = $this->deliveries->create($secondRouteId, $subscriptionId);

        $reserved = $this->deliveries->reserveNext($subscriptionId, 'consumer-a', 'tag-a');

        self::assertNotNull($reserved);
        self::assertSame($first->id, $reserved->id);
        self::assertNotSame($second->id, $reserved->id);
        self::assertSame(DeliveryState::Reserved, $reserved->state);
        self::assertSame('consumer-a', $reserved->consumerId);
        self::assertSame('tag-a', $reserved->deliveryTag);
        self::assertNotNull($reserved->reservedAt);
        self::assertSame(1, $reserved->attempts);
    }

    public function testFutureDeliveryIsNotReservedEarlyAndNoAvailableDeliveryReturnsNull(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('future-reserve');
        $this->deliveries->create($routeId, $subscriptionId, new DateTimeImmutable('2030-01-01T00:00:00+00:00'));

        self::assertNull($this->deliveries->reserveNext($subscriptionId, 'consumer-a'));
        self::assertNull($this->deliveries->reserveNext(999999, 'consumer-a'));
    }

    public function testReservationAfterReleaseIncrementsAttemptsAgain(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('attempts');
        $this->deliveries->create($routeId, $subscriptionId);

        $first = $this->deliveries->reserveNext($subscriptionId, 'consumer-a');
        self::assertNotNull($first);

        $released = $this->deliveries->release($first->id);
        $second = $this->deliveries->reserveNext($subscriptionId, 'consumer-b');

        self::assertSame(1, $released->attempts);
        self::assertNotNull($second);
        self::assertSame($first->id, $second->id);
        self::assertSame(2, $second->attempts);
        self::assertSame('consumer-b', $second->consumerId);
    }

    public function testAcknowledgePerformsReservedToAcknowledgedOnly(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('ack');
        $pending = $this->deliveries->create($routeId, $subscriptionId);

        $this->expectException(DeliveryStateException::class);
        $this->deliveries->acknowledge($pending->id);
    }

    public function testAcknowledgeReservedDeliveryIsTerminal(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('ack-terminal');
        $this->deliveries->create($routeId, $subscriptionId);
        $reserved = $this->deliveries->reserveNext($subscriptionId, 'consumer-a');
        self::assertNotNull($reserved);

        $acknowledged = $this->deliveries->acknowledge($reserved->id);

        self::assertSame(DeliveryState::Acknowledged, $acknowledged->state);
        self::assertNotNull($acknowledged->acknowledgedAt);
        self::assertSame('consumer-a', $acknowledged->consumerId);
        self::assertNull($this->deliveries->reserveNext($subscriptionId, 'consumer-b'));

        $this->expectException(DeliveryStateException::class);
        $this->deliveries->release($acknowledged->id);
    }

    public function testRejectedToAcknowledgedFails(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('reject-ack');
        $this->deliveries->create($routeId, $subscriptionId);
        $reserved = $this->deliveries->reserveNext($subscriptionId, 'consumer-a');
        self::assertNotNull($reserved);

        $rejected = $this->deliveries->reject($reserved->id);

        self::assertSame(DeliveryState::Rejected, $rejected->state);
        self::assertNull($rejected->acknowledgedAt);
        self::assertNull($this->deliveries->reserveNext($subscriptionId, 'consumer-b'));

        $this->expectException(DeliveryStateException::class);
        $this->deliveries->acknowledge($rejected->id);
    }

    public function testPendingToRejectedFails(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('pending-reject');
        $pending = $this->deliveries->create($routeId, $subscriptionId);

        $this->expectException(DeliveryStateException::class);
        $this->deliveries->reject($pending->id);
    }

    public function testReleasePerformsReservedToPendingAndClearsRuntimeReservationData(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('release');
        $this->deliveries->create($routeId, $subscriptionId);
        $reserved = $this->deliveries->reserveNext($subscriptionId, 'consumer-a', 'tag-a');
        self::assertNotNull($reserved);

        $released = $this->deliveries->release($reserved->id);

        self::assertSame(DeliveryState::Pending, $released->state);
        self::assertNull($released->consumerId);
        self::assertNull($released->deliveryTag);
        self::assertNull($released->reservedAt);
        self::assertSame(1, $released->attempts);

        $reservedAgain = $this->deliveries->reserveNext($subscriptionId, 'consumer-b');
        self::assertNotNull($reservedAgain);
        self::assertSame($released->id, $reservedAgain->id);
    }

    public function testExplicitDelayedReleaseIsNotImmediatelyReservable(): void
    {
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('delayed-release');
        $this->deliveries->create($routeId, $subscriptionId);
        $reserved = $this->deliveries->reserveNext($subscriptionId, 'consumer-a', 'tag-a');
        self::assertNotNull($reserved);

        $availableAt = new DateTimeImmutable('2030-01-01T00:00:00+00:00');
        $released = $this->deliveries->release($reserved->id, $availableAt);

        self::assertSameTimestamp($availableAt, $released->availableAt);
        self::assertNull($this->deliveries->reserveNext($subscriptionId, 'consumer-b'));
    }

    public function testUnknownDeliveryTransitionFailsClearly(): void
    {
        $this->expectException(DeliveryStateException::class);
        $this->expectExceptionMessage('Delivery 999999 does not exist.');

        $this->deliveries->acknowledge(999999);
    }

    public function testConcurrentReservationsSkipLockedRowsAndDoNotClaimTheSameDelivery(): void
    {
        $dsn = (string) getenv('FLUXQ_TEST_DATABASE_URL');
        [$routeId, $subscriptionId] = $this->createRouteAndSubscription('concurrent');
        $secondRouteId = $this->createRouteForExistingSubscriptionDestination($subscriptionId);
        $first = $this->deliveries->create($routeId, $subscriptionId);
        $second = $this->deliveries->create($secondRouteId, $subscriptionId);

        $connectionA = Connection::fromDsn($dsn);
        $connectionB = Connection::fromDsn($dsn);
        $pdoA = $connectionA->pdo();
        $repositoryB = new DeliveryRepository($connectionB);

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
            self::assertSame($first->id, (int) $lock->fetchColumn());

            $update = $pdoA->prepare(<<<'SQL'
UPDATE deliveries
SET state = 'reserved',
    consumer_id = 'consumer-a',
    reserved_at = CURRENT_TIMESTAMP,
    attempts = attempts + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :id
SQL);
            $update->execute(['id' => $first->id]);

            $reservedByB = $repositoryB->reserveNext($subscriptionId, 'consumer-b');

            self::assertNotNull($reservedByB);
            self::assertSame($second->id, $reservedByB->id);
            self::assertNotSame($first->id, $reservedByB->id);
            self::assertSame(DeliveryState::Reserved, $reservedByB->state);
            self::assertSame('consumer-b', $reservedByB->consumerId);
        } finally {
            if ($pdoA->inTransaction()) {
                $pdoA->rollBack();
            }
        }
    }

    public function testReadyCountIncludesOnlyPendingAvailableDeliveriesForDestination(): void
    {
        [$routeId, $subscriptionId, $destinationId] = $this->createRouteAndSubscription('ready-count');
        $secondRouteId = $this->createRouteForExistingSubscriptionDestination($subscriptionId);
        $thirdRouteId = $this->createRouteForExistingSubscriptionDestination($subscriptionId);
        $futureRouteId = $this->createRouteForExistingSubscriptionDestination($subscriptionId);

        $this->deliveries->create($routeId, $subscriptionId);
        $reserved = $this->deliveries->create($secondRouteId, $subscriptionId);
        $acknowledged = $this->deliveries->create($thirdRouteId, $subscriptionId);
        $this->deliveries->create($futureRouteId, $subscriptionId, new DateTimeImmutable('2030-01-01T00:00:00+00:00'));

        $reserved = $this->deliveries->reserveNext($subscriptionId, 'consumer-a');
        self::assertNotNull($reserved);
        $acknowledged = $this->deliveries->reserveNext($subscriptionId, 'consumer-b');
        self::assertNotNull($acknowledged);
        $this->deliveries->acknowledge($acknowledged->id);

        self::assertSame(1, $this->deliveries->countReadyByDestination($destinationId));
    }

    /**
     * @return array{0: int, 1: int, 2: int}
     */
    private function createRouteAndSubscription(string $name): array
    {
        $this->sequence++;
        $destination = $this->destinations->create(
            $this->defaultVirtualHostId,
            sprintf('%s-%d', $name, $this->sequence),
            'queue'
        );
        $message = $this->messages->create(sprintf('payload-%s-%d', $name, $this->sequence));
        $route = $this->routes->create($message->id, $destination->id);
        $subscription = $this->subscriptions->create($destination->id, 'default');

        return [$route->id, $subscription->id, $destination->id];
    }

    private function createRouteForExistingSubscriptionDestination(int $subscriptionId): int
    {
        $subscription = $this->subscriptions->findById($subscriptionId)
            ?? throw new \RuntimeException(sprintf('Subscription %d was not found.', $subscriptionId));
        $message = $this->messages->create(sprintf('payload-extra-%d', ++$this->sequence));
        $route = $this->routes->create($message->id, $subscription->destinationId);

        return $route->id;
    }

    private static function assertSameTimestamp(DateTimeImmutable $expected, DateTimeImmutable $actual): void
    {
        self::assertSame(
            $expected->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u'),
            $actual->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u')
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
