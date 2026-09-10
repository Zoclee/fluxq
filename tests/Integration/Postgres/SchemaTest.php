<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\MigrationFailure;
use FluxQ\Persistence\Postgres\Migrator;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class SchemaTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;

    #[Before]
    public function setUpSchema(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');

        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL schema integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();

        $this->assertSafeTestDatabase();
        $this->resetSchema();
        $this->applyMigrations();
    }

    public function testDefaultVirtualHostExistsExactlyOnce(): void
    {
        self::assertSame(
            1,
            (int) $this->pdo->query("SELECT count(*) FROM virtual_hosts WHERE name = '/'")->fetchColumn()
        );
    }

    public function testEveryMigrationIsRecorded(): void
    {
        self::assertSame(
            16,
            (int) $this->pdo->query('SELECT count(*) FROM schema_migrations')->fetchColumn()
        );
    }

    public function testMigrationsAreIdempotent(): void
    {
        $result = $this->applyMigrations();

        self::assertSame([], $result->applied);

        self::assertSame(
            16,
            (int) $this->pdo->query('SELECT count(*) FROM schema_migrations')->fetchColumn()
        );
        self::assertSame(
            1,
            (int) $this->pdo->query("SELECT count(*) FROM virtual_hosts WHERE name = '/'")->fetchColumn()
        );
    }

    public function testMigrationOrderMatchesLexicalFilenameOrder(): void
    {
        $statement = $this->pdo->query('SELECT version FROM schema_migrations ORDER BY applied_at, version');
        self::assertNotFalse($statement);

        self::assertSame(
            [
                '20260820_120000_create_schema_migrations',
                '20260820_120001_create_virtual_hosts',
                '20260820_120002_create_destinations',
                '20260820_120003_create_bindings',
                '20260820_120004_create_messages',
                '20260820_120005_create_message_routes',
                '20260820_120006_create_subscriptions',
                '20260820_120007_create_deliveries',
                '20260820_120008_enforce_binding_destination_virtual_host',
                '20260820_120009_enforce_delivery_route_subscription_destination',
                '20260820_120010_create_routing_sources',
                '20260820_120011_create_users',
                '20260820_120012_create_user_permissions',
                '20260820_120013_allow_fanout_routing_sources',
                '20260820_120014_allow_topic_routing_sources',
                '20260820_120015_add_message_metadata',
            ],
            $statement->fetchAll(PDO::FETCH_COLUMN)
        );
    }

    public function testFailedMigrationIsNotRecordedAndStopsSubsequentMigrations(): void
    {
        $this->resetSchema();

        $migrationDirectory = $this->createTemporaryMigrationDirectory([
            '20260820_130000_create_probe.sql' => 'CREATE TABLE probe (id integer PRIMARY KEY);',
            '20260820_130001_fail_probe.sql' => 'INSERT INTO missing_table (id) VALUES (1);',
            '20260820_130002_after_failure.sql' => 'CREATE TABLE after_failure (id integer PRIMARY KEY);',
        ]);

        try {
            (new Migrator($this->connection, $migrationDirectory))->migrate();
            self::fail('Expected failed migration to throw.');
        } catch (MigrationFailure $exception) {
            self::assertSame('20260820_130001_fail_probe.sql', $exception->migration);
        }

        self::assertSame(
            ['20260820_130000_create_probe'],
            $this->pdo->query('SELECT version FROM schema_migrations ORDER BY version')->fetchAll(PDO::FETCH_COLUMN)
        );
        self::assertSame('probe', $this->toRegclass('probe'));
        self::assertNull($this->toRegclass('after_failure'));
    }

    public function testFailedTransactionalMigrationIsRolledBack(): void
    {
        $this->resetSchema();

        $migrationDirectory = $this->createTemporaryMigrationDirectory([
            '20260820_140000_create_then_fail.sql' => <<<'SQL'
CREATE TABLE rolled_back_probe (id integer PRIMARY KEY);
INSERT INTO missing_table (id) VALUES (1);
SQL,
        ]);

        $this->expectException(MigrationFailure::class);

        try {
            (new Migrator($this->connection, $migrationDirectory))->migrate();
        } finally {
            self::assertSame([], $this->pdo->query('SELECT version FROM schema_migrations')->fetchAll(PDO::FETCH_COLUMN));
            self::assertNull($this->toRegclass('rolled_back_probe'));
        }
    }

    public function testVirtualHostNamesAreUnique(): void
    {
        $this->expectConstraintViolation();

        $this->pdo->exec("INSERT INTO virtual_hosts (name) VALUES ('/')");
    }

    public function testDestinationNamesAreUniqueWithinVirtualHostOnly(): void
    {
        $defaultHostId = $this->virtualHostId('/');
        $otherHostId = $this->insertVirtualHost('tenant-a');

        $this->insertDestination($defaultHostId, 'orders');
        $this->insertDestination($otherHostId, 'orders');

        $this->expectConstraintViolation();

        $this->insertDestination($defaultHostId, 'orders');
    }

    public function testInvalidDestinationTypeFails(): void
    {
        $this->expectConstraintViolation();

        $this->insertDestination($this->virtualHostId('/'), 'topic-a', 'topic');
    }

    public function testBindingDestinationMustBelongToSameVirtualHost(): void
    {
        $defaultHostId = $this->virtualHostId('/');
        $otherHostId = $this->insertVirtualHost('tenant-a');
        $otherDestinationId = $this->insertDestination($otherHostId, 'orders');

        $this->expectConstraintViolation();

        $statement = $this->pdo->prepare(
            "INSERT INTO bindings (virtual_host_id, source, destination_id, routing_key)
             VALUES (:virtual_host_id, 'orders', :destination_id, 'order.created')"
        );
        $statement->execute([
            'virtual_host_id' => $defaultHostId,
            'destination_id' => $otherDestinationId,
        ]);
    }

    public function testBinaryPayloadCanBeStoredAndOneMessageCanHaveMultipleRoutes(): void
    {
        $hostId = $this->virtualHostId('/');
        $destinationAId = $this->insertDestination($hostId, 'queue-a');
        $destinationBId = $this->insertDestination($hostId, 'queue-b');
        $messageId = $this->insertMessage(hex2bin('000102ff') ?: '');

        $routeAId = $this->insertMessageRoute($messageId, $destinationAId);
        $routeBId = $this->insertMessageRoute($messageId, $destinationBId);

        self::assertNotSame($routeAId, $routeBId);
        self::assertSame(2, (int) $this->pdo->query('SELECT count(*) FROM message_routes')->fetchColumn());
        self::assertSame('000102ff', $this->pdo->query("SELECT encode(payload, 'hex') FROM messages")->fetchColumn());
    }

    public function testInvalidDeliveryStateFails(): void
    {
        [$messageRouteId, $subscriptionId, $destinationId] = $this->createRoutedSubscription();

        $this->expectConstraintViolation();
        $this->pdo->exec(sprintf(
            "INSERT INTO deliveries (message_route_id, subscription_id, destination_id, state) VALUES (%d, %d, %d, 'unknown')",
            $messageRouteId,
            $subscriptionId,
            $destinationId
        ));
    }

    public function testNegativeDeliveryAttemptsFail(): void
    {
        [$messageRouteId, $subscriptionId, $destinationId] = $this->createRoutedSubscription();

        $this->expectConstraintViolation();
        $this->pdo->exec(sprintf(
            'INSERT INTO deliveries (message_route_id, subscription_id, destination_id, attempts) VALUES (%d, %d, %d, -1)',
            $messageRouteId,
            $subscriptionId,
            $destinationId
        ));
    }

    public function testDeliveryRouteAndSubscriptionMustUseSameDestination(): void
    {
        $hostId = $this->virtualHostId('/');
        $routeDestinationId = $this->insertDestination($hostId, 'route-destination');
        $subscriptionDestinationId = $this->insertDestination($hostId, 'subscription-destination');
        $messageId = $this->insertMessage(hex2bin('bb') ?: '');
        $messageRouteId = $this->insertMessageRoute($messageId, $routeDestinationId);

        $statement = $this->pdo->prepare(
            "INSERT INTO subscriptions (destination_id, name) VALUES (:destination_id, 'default') RETURNING id"
        );
        $statement->execute(['destination_id' => $subscriptionDestinationId]);
        $subscriptionId = (int) $statement->fetchColumn();

        $this->expectConstraintViolation();
        $insert = $this->pdo->prepare(
            'INSERT INTO deliveries (message_route_id, subscription_id, destination_id)
             VALUES (:message_route_id, :subscription_id, :destination_id)'
        );
        $insert->execute([
            'message_route_id' => $messageRouteId,
            'subscription_id' => $subscriptionId,
            'destination_id' => $routeDestinationId,
        ]);
    }

    public function testForeignKeyIntegrityIsEnforced(): void
    {
        $this->expectConstraintViolation();

        $this->pdo->exec(
            "INSERT INTO destinations (virtual_host_id, name, type) VALUES (999999, 'orphan', 'queue')"
        );
    }

    public function testExpectedIndexesExist(): void
    {
        $indexes = $this->pdo->query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
        )->fetchAll(PDO::FETCH_COLUMN);

        self::assertContains('destinations_virtual_host_name_unique', $indexes);
        self::assertContains('bindings_route_lookup_idx', $indexes);
        self::assertContains('messages_message_id_unique', $indexes);
        self::assertContains('message_routes_destination_available_idx', $indexes);
        self::assertContains('subscriptions_destination_name_unique', $indexes);
        self::assertContains('deliveries_pending_claim_idx', $indexes);
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function applyMigrations(): \FluxQ\Persistence\Postgres\MigrationResult
    {
        return (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();
    }

    private function virtualHostId(string $name): int
    {
        $statement = $this->pdo->prepare('SELECT id FROM virtual_hosts WHERE name = :name');
        $statement->execute(['name' => $name]);

        return (int) $statement->fetchColumn();
    }

    private function insertVirtualHost(string $name): int
    {
        $statement = $this->pdo->prepare('INSERT INTO virtual_hosts (name) VALUES (:name) RETURNING id');
        $statement->execute(['name' => $name]);

        return (int) $statement->fetchColumn();
    }

    private function insertDestination(int $virtualHostId, string $name, string $type = 'queue'): int
    {
        $statement = $this->pdo->prepare(
            'INSERT INTO destinations (virtual_host_id, name, type) VALUES (:virtual_host_id, :name, :type) RETURNING id'
        );
        $statement->execute([
            'virtual_host_id' => $virtualHostId,
            'name' => $name,
            'type' => $type,
        ]);

        return (int) $statement->fetchColumn();
    }

    private function insertMessage(string $payload): int
    {
        $statement = $this->pdo->prepare(
            'INSERT INTO messages (message_id, payload) VALUES (:message_id, :payload) RETURNING id'
        );
        $statement->bindValue('message_id', '00000000-0000-4000-8000-000000000001');
        $statement->bindValue('payload', $payload, PDO::PARAM_LOB);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    private function insertMessageRoute(int $messageId, int $destinationId): int
    {
        $statement = $this->pdo->prepare(
            'INSERT INTO message_routes (message_id, destination_id) VALUES (:message_id, :destination_id) RETURNING id'
        );
        $statement->execute([
            'message_id' => $messageId,
            'destination_id' => $destinationId,
        ]);

        return (int) $statement->fetchColumn();
    }

    /**
     * @return array{0: int, 1: int, 2: int}
     */
    private function createRoutedSubscription(): array
    {
        $hostId = $this->virtualHostId('/');
        $destinationId = $this->insertDestination($hostId, 'deliveries');
        $messageId = $this->insertMessage(hex2bin('aa') ?: '');
        $messageRouteId = $this->insertMessageRoute($messageId, $destinationId);

        $statement = $this->pdo->prepare(
            "INSERT INTO subscriptions (destination_id, name) VALUES (:destination_id, 'default') RETURNING id"
        );
        $statement->execute(['destination_id' => $destinationId]);

        return [$messageRouteId, (int) $statement->fetchColumn(), $destinationId];
    }

    private function expectConstraintViolation(): void
    {
        $this->expectException(PDOException::class);
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

    /**
     * @param array<string, string> $migrations
     */
    private function createTemporaryMigrationDirectory(array $migrations): string
    {
        $directory = sys_get_temp_dir() . '/fluxq_migrations_' . bin2hex(random_bytes(8));

        if (!mkdir($directory) && !is_dir($directory)) {
            throw new RuntimeException(sprintf('Could not create temporary migration directory: %s', $directory));
        }

        foreach ($migrations as $filename => $sql) {
            file_put_contents($directory . DIRECTORY_SEPARATOR . $filename, $sql);
        }

        return $directory;
    }

    private function toRegclass(string $table): ?string
    {
        $statement = $this->pdo->prepare('SELECT to_regclass(:table)');
        $statement->execute(['table' => $table]);
        $result = $statement->fetchColumn();

        return is_string($result) ? $result : null;
    }
}
