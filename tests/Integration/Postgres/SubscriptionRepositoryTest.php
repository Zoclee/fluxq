<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use InvalidArgumentException;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class SubscriptionRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private DestinationRepository $destinations;
    private SubscriptionRepository $subscriptions;
    private int $defaultVirtualHostId;

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
        $this->destinations = new DestinationRepository($this->connection);
        $this->subscriptions = new SubscriptionRepository($this->connection);
        $this->defaultVirtualHostId = $virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testSubscriptionCanBeCreatedWithDurableTrueAndMetadata(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        $subscription = $this->subscriptions->create(
            $destination->id,
            'workers',
            durable: true,
            metadata: ['priority' => 'standard']
        );

        self::assertGreaterThan(0, $subscription->id);
        self::assertSame($destination->id, $subscription->destinationId);
        self::assertSame('workers', $subscription->name);
        self::assertTrue($subscription->durable);
        self::assertSame(['priority' => 'standard'], $subscription->metadata);
        self::assertNotSame('', $subscription->createdAt->format(DATE_ATOM));
        self::assertNotSame('', $subscription->updatedAt->format(DATE_ATOM));
        self::assertSame(0, $this->countRows('deliveries'));
    }

    public function testDurableFalseRoundTrips(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'ephemeral', 'queue');

        $subscription = $this->subscriptions->create($destination->id, 'workers', durable: false);
        $found = $this->subscriptions->findById($subscription->id);

        self::assertFalse($subscription->durable);
        self::assertNotNull($found);
        self::assertFalse($found->durable);
    }

    public function testMetadataRoundTripsIncludingNestedJsonCompatibleValues(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'metadata', 'queue');
        $metadata = [
            'string' => 'value',
            'integer' => 42,
            'float' => 12.5,
            'boolean' => true,
            'null' => null,
            'nested' => [
                'object' => ['key' => 'value'],
                'list' => ['a', 'b'],
            ],
        ];

        $subscription = $this->subscriptions->create($destination->id, 'workers', metadata: $metadata);
        $found = $this->subscriptions->findById($subscription->id);

        self::assertNotNull($found);
        self::assertEquals($metadata, $found->metadata);
    }

    public function testMetadataDefaultsToEmptyObjectAndListsAreRejected(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'empty-metadata', 'queue');

        $subscription = $this->subscriptions->create($destination->id, 'workers');

        self::assertSame([], $subscription->metadata);

        $this->expectException(InvalidArgumentException::class);

        $this->subscriptions->create($destination->id, 'list-metadata', metadata: ['not', 'an', 'object']);
    }

    public function testFindByIdReturnsSubscriptionAndUnknownIdReturnsNull(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'lookup-id', 'queue');
        $created = $this->subscriptions->create($destination->id, 'workers');

        $found = $this->subscriptions->findById($created->id);

        self::assertNotNull($found);
        self::assertSame($created->id, $found->id);
        self::assertSame($destination->id, $found->destinationId);
        self::assertSame('workers', $found->name);
        self::assertNull($this->subscriptions->findById(999999));
    }

    public function testFindByNameIsScopedToDestinationAndUnknownNameReturnsNull(): void
    {
        $destinationA = $this->destinations->create($this->defaultVirtualHostId, 'lookup-a', 'queue');
        $destinationB = $this->destinations->create($this->defaultVirtualHostId, 'lookup-b', 'queue');
        $created = $this->subscriptions->create($destinationA->id, 'workers');
        $this->subscriptions->create($destinationB->id, 'workers');

        self::assertSame($created->id, $this->subscriptions->findByName($destinationA->id, 'workers')?->id);
        self::assertNull($this->subscriptions->findByName($destinationA->id, 'missing'));
        self::assertNull($this->subscriptions->findByName(999999, 'workers'));
    }

    public function testSameSubscriptionNameInDifferentDestinationsSucceeds(): void
    {
        $destinationA = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $destinationB = $this->destinations->create($this->defaultVirtualHostId, 'billing', 'queue');

        $first = $this->subscriptions->create($destinationA->id, 'workers');
        $second = $this->subscriptions->create($destinationB->id, 'workers');

        self::assertNotSame($first->id, $second->id);
        self::assertSame($first->id, $this->subscriptions->findByName($destinationA->id, 'workers')?->id);
        self::assertSame($second->id, $this->subscriptions->findByName($destinationB->id, 'workers')?->id);
    }

    public function testDuplicateNameInSameDestinationFails(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'duplicates', 'queue');
        $this->subscriptions->create($destination->id, 'workers');

        $this->expectException(PDOException::class);

        $this->subscriptions->create($destination->id, 'workers');
    }

    public function testUnknownDestinationIdFails(): void
    {
        $this->expectException(PDOException::class);

        $this->subscriptions->create(999999, 'orphan');
    }

    public function testAllByDestinationReturnsOnlyMatchingSubscriptionsInNameOrder(): void
    {
        $destinationA = $this->destinations->create($this->defaultVirtualHostId, 'destination-a', 'queue');
        $destinationB = $this->destinations->create($this->defaultVirtualHostId, 'destination-b', 'queue');

        $this->subscriptions->create($destinationA->id, 'z-last');
        $this->subscriptions->create($destinationA->id, 'a-first');
        $this->subscriptions->create($destinationB->id, 'm-other');

        self::assertSame(
            ['a-first', 'z-last'],
            array_map(
                static fn ($subscription): string => $subscription->name,
                $this->subscriptions->allByDestination($destinationA->id)
            )
        );
    }

    public function testDeleteReturnsTrueForExistingSubscriptionAndFalseForUnknownSubscription(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'delete', 'queue');
        $subscription = $this->subscriptions->create($destination->id, 'workers');

        self::assertTrue($this->subscriptions->delete($subscription->id));
        self::assertNull($this->subscriptions->findById($subscription->id));
        self::assertFalse($this->subscriptions->delete($subscription->id));
        self::assertFalse($this->subscriptions->delete(999999));
    }

    public function testDeletingSubscriptionWithDeliveryIsBlockedByExistingForeignKey(): void
    {
        $messages = new MessageRepository($this->connection);
        $routes = new MessageRouteRepository($this->connection);
        $message = $messages->create('payload');
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'delivery-fk', 'queue');
        $route = $routes->create($message->id, $destination->id);
        $subscription = $this->subscriptions->create($destination->id, 'workers');

        $statement = $this->pdo->prepare(
            'INSERT INTO deliveries (message_route_id, subscription_id, destination_id)
             VALUES (:message_route_id, :subscription_id, :destination_id)'
        );
        $statement->execute([
            'message_route_id' => $route->id,
            'subscription_id' => $subscription->id,
            'destination_id' => $destination->id,
        ]);

        $this->expectException(PDOException::class);

        $this->subscriptions->delete($subscription->id);
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function countRows(string $table): int
    {
        return (int) $this->pdo->query('SELECT count(*) FROM ' . $table)->fetchColumn();
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
