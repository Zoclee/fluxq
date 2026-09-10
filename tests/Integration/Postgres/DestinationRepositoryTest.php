<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Broker\DestinationType;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use InvalidArgumentException;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class DestinationRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private VirtualHostRepository $virtualHosts;
    private DestinationRepository $destinations;
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
        $this->defaultVirtualHostId = $this->virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testDestinationCanBeCreatedInDefaultVirtualHost(): void
    {
        $destination = $this->destinations->create(
            $this->defaultVirtualHostId,
            'orders',
            DestinationType::Queue,
            durable: true,
            autoDelete: false,
            metadata: ['priority' => 'standard', 'limits' => ['max' => 100]]
        );

        self::assertGreaterThan(0, $destination->id);
        self::assertSame($this->defaultVirtualHostId, $destination->virtualHostId);
        self::assertSame('orders', $destination->name);
        self::assertSame(DestinationType::Queue, $destination->type);
        self::assertTrue($destination->durable);
        self::assertFalse($destination->autoDelete);
        self::assertEquals(['priority' => 'standard', 'limits' => ['max' => 100]], $destination->metadata);
        self::assertNotSame('', $destination->createdAt->format(DATE_ATOM));
        self::assertNotSame('', $destination->updatedAt->format(DATE_ATOM));
    }

    public function testFindByNameFindsScopedDestinationAndUnknownReturnsNull(): void
    {
        $created = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        self::assertSame($created->id, $this->destinations->findByName($this->defaultVirtualHostId, 'orders')?->id);
        self::assertNull($this->destinations->findByName($this->defaultVirtualHostId, 'missing'));
        self::assertNull($this->destinations->findByName(999999, 'orders'));
    }

    public function testDuplicateNameInSameVirtualHostFails(): void
    {
        $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        $this->expectException(PDOException::class);

        $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
    }

    public function testSameDestinationNameInDifferentVirtualHostsSucceeds(): void
    {
        $otherVirtualHost = $this->virtualHosts->create('tenant-a');

        $defaultDestination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $otherDestination = $this->destinations->create($otherVirtualHost->id, 'orders', 'queue');

        self::assertNotSame($defaultDestination->id, $otherDestination->id);
        self::assertSame($defaultDestination->id, $this->destinations->findByName($this->defaultVirtualHostId, 'orders')?->id);
        self::assertSame($otherDestination->id, $this->destinations->findByName($otherVirtualHost->id, 'orders')?->id);
    }

    public function testInvalidDestinationTypeFails(): void
    {
        $this->expectException(PDOException::class);

        $this->destinations->create($this->defaultVirtualHostId, 'events', 'topic');
    }

    public function testNonexistentVirtualHostIdFails(): void
    {
        $this->expectException(PDOException::class);

        $this->destinations->create(999999, 'orphan', 'queue');
    }

    public function testAllByVirtualHostReturnsOnlyMatchingDestinationsInNameOrder(): void
    {
        $otherVirtualHost = $this->virtualHosts->create('tenant-a');

        $this->destinations->create($this->defaultVirtualHostId, 'z-last', 'queue');
        $this->destinations->create($this->defaultVirtualHostId, 'a-first', 'queue');
        $this->destinations->create($otherVirtualHost->id, 'm-other', 'queue');

        self::assertSame(
            ['a-first', 'z-last'],
            array_map(
                static fn ($destination): string => $destination->name,
                $this->destinations->allByVirtualHost($this->defaultVirtualHostId)
            )
        );
    }

    public function testMetadataDefaultsToEmptyObjectAndListsAreRejected(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'empty-metadata', 'queue');

        self::assertSame([], $destination->metadata);

        $this->expectException(InvalidArgumentException::class);

        $this->destinations->create($this->defaultVirtualHostId, 'list-metadata', 'queue', metadata: ['not', 'an', 'object']);
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
