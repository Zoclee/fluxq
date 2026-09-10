<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class VirtualHostRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private VirtualHostRepository $repository;

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

        $this->repository = new VirtualHostRepository($this->connection);
    }

    public function testDefaultVirtualHostCanBeFoundAfterMigrations(): void
    {
        $virtualHost = $this->repository->findByName('/');

        self::assertNotNull($virtualHost);
        self::assertSame('/', $virtualHost->name);
        self::assertGreaterThan(0, $virtualHost->id);
        self::assertNotSame('', $virtualHost->createdAt->format(DATE_ATOM));
    }

    public function testFindByNameReturnsNullForUnknownName(): void
    {
        self::assertNull($this->repository->findByName('missing'));
    }

    public function testCreatePersistsAndReturnsVirtualHost(): void
    {
        $virtualHost = $this->repository->create('tenant-a');

        self::assertGreaterThan(0, $virtualHost->id);
        self::assertSame('tenant-a', $virtualHost->name);
        self::assertNotSame('', $virtualHost->createdAt->format(DATE_ATOM));
        self::assertSame($virtualHost->id, $this->repository->findByName('tenant-a')?->id);
    }

    public function testDuplicateVirtualHostNamesFail(): void
    {
        $this->repository->create('tenant-a');

        $this->expectException(PDOException::class);

        $this->repository->create('tenant-a');
    }

    public function testAllReturnsVirtualHostsInDeterministicNameOrder(): void
    {
        $this->repository->create('tenant-b');
        $this->repository->create('tenant-a');

        self::assertSame(
            ['/', 'tenant-a', 'tenant-b'],
            array_map(static fn ($virtualHost): string => $virtualHost->name, $this->repository->all())
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
