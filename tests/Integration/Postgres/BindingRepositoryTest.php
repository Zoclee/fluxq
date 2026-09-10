<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use InvalidArgumentException;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class BindingRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private VirtualHostRepository $virtualHosts;
    private DestinationRepository $destinations;
    private BindingRepository $bindings;
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

        $this->virtualHosts = new VirtualHostRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $this->bindings = new BindingRepository($this->connection);
        $this->defaultVirtualHostId = $this->virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testBindingCanBeCreatedAndFoundById(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'order-workers', 'queue');

        $binding = $this->bindings->create(
            $this->defaultVirtualHostId,
            'orders',
            $destination->id,
            'order.created',
            ['priority' => 'standard']
        );

        self::assertGreaterThan(0, $binding->id);
        self::assertSame($this->defaultVirtualHostId, $binding->virtualHostId);
        self::assertSame('orders', $binding->source);
        self::assertSame($destination->id, $binding->destinationId);
        self::assertSame('order.created', $binding->routingKey);
        self::assertSame(['priority' => 'standard'], $binding->metadata);
        self::assertNotSame('', $binding->createdAt->format(DATE_ATOM));

        self::assertSame($binding->id, $this->bindings->findById($binding->id)?->id);
        self::assertNull($this->bindings->findById(999999));
    }

    public function testFindForRouteReturnsOnlyExactTupleMatchesInDeterministicOrder(): void
    {
        $destinationA = $this->destinations->create($this->defaultVirtualHostId, 'worker-a', 'queue');
        $destinationB = $this->destinations->create($this->defaultVirtualHostId, 'worker-b', 'queue');
        $differentSource = $this->destinations->create($this->defaultVirtualHostId, 'source-worker', 'queue');
        $differentRoutingKey = $this->destinations->create($this->defaultVirtualHostId, 'key-worker', 'queue');
        $otherVirtualHost = $this->virtualHosts->create('tenant-a');
        $otherDestination = $this->destinations->create($otherVirtualHost->id, 'worker-a', 'queue');

        $first = $this->bindings->create($this->defaultVirtualHostId, 'orders', $destinationA->id, 'order.created');
        $second = $this->bindings->create($this->defaultVirtualHostId, 'orders', $destinationB->id, 'order.created');
        $this->bindings->create($this->defaultVirtualHostId, 'billing', $differentSource->id, 'order.created');
        $this->bindings->create($this->defaultVirtualHostId, 'orders', $differentRoutingKey->id, 'order.cancelled');
        $this->bindings->create($otherVirtualHost->id, 'orders', $otherDestination->id, 'order.created');

        self::assertSame(
            [$first->id, $second->id],
            array_map(
                static fn ($binding): int => $binding->id,
                $this->bindings->findForRoute($this->defaultVirtualHostId, 'orders', 'order.created')
            )
        );
    }

    public function testAllByDestinationReturnsOnlyDestinationBindingsInDeterministicOrder(): void
    {
        $destinationA = $this->destinations->create($this->defaultVirtualHostId, 'worker-a', 'queue');
        $destinationB = $this->destinations->create($this->defaultVirtualHostId, 'worker-b', 'queue');

        $first = $this->bindings->create($this->defaultVirtualHostId, 'orders', $destinationA->id, 'order.created');
        $second = $this->bindings->create($this->defaultVirtualHostId, 'billing', $destinationA->id, 'invoice.created');
        $this->bindings->create($this->defaultVirtualHostId, 'orders', $destinationB->id, 'order.created');

        self::assertSame(
            [$first->id, $second->id],
            array_map(
                static fn ($binding): int => $binding->id,
                $this->bindings->allByDestination($destinationA->id)
            )
        );
    }

    public function testDuplicateBindingFails(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'worker-a', 'queue');
        $this->bindings->create($this->defaultVirtualHostId, 'orders', $destination->id, 'order.created');

        $this->expectException(PDOException::class);

        $this->bindings->create($this->defaultVirtualHostId, 'orders', $destination->id, 'order.created');
    }

    public function testUnknownVirtualHostFails(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'worker-a', 'queue');

        $this->expectException(PDOException::class);

        $this->bindings->create(999999, 'orders', $destination->id, 'order.created');
    }

    public function testUnknownDestinationFails(): void
    {
        $this->expectException(PDOException::class);

        $this->bindings->create($this->defaultVirtualHostId, 'orders', 999999, 'order.created');
    }

    public function testDestinationMustBelongToBindingVirtualHost(): void
    {
        $otherVirtualHost = $this->virtualHosts->create('tenant-a');
        $otherDestination = $this->destinations->create($otherVirtualHost->id, 'worker-a', 'queue');

        $this->expectException(PDOException::class);

        $this->bindings->create($this->defaultVirtualHostId, 'orders', $otherDestination->id, 'order.created');
    }

    public function testDeleteReturnsTrueForExistingBindingAndFalseForUnknownBinding(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'worker-a', 'queue');
        $binding = $this->bindings->create($this->defaultVirtualHostId, 'orders', $destination->id, 'order.created');

        self::assertTrue($this->bindings->delete($binding->id));
        self::assertNull($this->bindings->findById($binding->id));
        self::assertFalse($this->bindings->delete($binding->id));
        self::assertFalse($this->bindings->delete(999999));
    }

    public function testMetadataDefaultsToEmptyObjectAndListsAreRejected(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'worker-a', 'queue');

        $binding = $this->bindings->create($this->defaultVirtualHostId, 'orders', $destination->id, 'order.created');

        self::assertSame([], $binding->metadata);

        $this->expectException(InvalidArgumentException::class);

        $this->bindings->create(
            $this->defaultVirtualHostId,
            'orders',
            $destination->id,
            'order.cancelled',
            ['not', 'an', 'object']
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
