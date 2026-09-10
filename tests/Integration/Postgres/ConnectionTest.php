<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Console\Commands\DbStatusCommand;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Persistence\Postgres\Migrator;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ConnectionTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private ConnectionConfig $config;

    #[Before]
    public function connect(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');

        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL connection integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();
        $this->config = $this->connection->config();

        $this->assertSafeTestDatabase();
    }

    public function testSuccessfulConnectionRetrievesPostgreSQLVersion(): void
    {
        $version = $this->pdo->query('SELECT version()')->fetchColumn();

        self::assertIsString($version);
        self::assertStringContainsString('PostgreSQL', $version);
    }

    public function testTransactionCommitsAndReturnsCallbackValue(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS transaction_probe');
        $this->pdo->exec('CREATE TABLE transaction_probe (id integer PRIMARY KEY)');

        $result = $this->connection->transaction(function (PDO $pdo): string {
            $pdo->exec('INSERT INTO transaction_probe (id) VALUES (1)');

            return 'committed';
        });

        self::assertSame('committed', $result);
        self::assertSame(1, (int) $this->pdo->query('SELECT count(*) FROM transaction_probe')->fetchColumn());
    }

    public function testTransactionRollsBackAndPreservesOriginalException(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS rollback_probe');
        $this->pdo->exec('CREATE TABLE rollback_probe (id integer PRIMARY KEY)');

        $exception = new RuntimeException('original failure');

        try {
            $this->connection->transaction(function (PDO $pdo) use ($exception): void {
                $pdo->exec('INSERT INTO rollback_probe (id) VALUES (1)');

                throw $exception;
            });

            self::fail('Expected transaction callback exception to be rethrown.');
        } catch (RuntimeException $caught) {
            self::assertSame($exception, $caught);
        }

        self::assertFalse($this->pdo->inTransaction());
        self::assertSame(0, (int) $this->pdo->query('SELECT count(*) FROM rollback_probe')->fetchColumn());
    }

    public function testNestedTransactionsFailClearly(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Nested PostgreSQL transactions are not supported.');

        $this->connection->transaction(function (): void {
            $this->connection->transaction(static function (): void {
            });
        });
    }

    public function testDbStatusRetrievesDatabaseInformationAndDoesNotExposePassword(): void
    {
        $this->resetSchema();
        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();

        $output = $this->runDbStatus();

        self::assertStringContainsString('Status:   connected', $output);
        self::assertStringContainsString('Driver:   PostgreSQL', $output);
        self::assertStringContainsString('Database: ' . $this->config->database, $output);
        self::assertStringContainsString('Version:  PostgreSQL', $output);
        self::assertStringContainsString('Migrations: up to date', $output);

        self::assertStringNotContainsString('Password:', $output);
        self::assertStringNotContainsString('password=', $output);
    }

    public function testDbStatusReportsPendingMigrationsWithoutApplyingThem(): void
    {
        $this->resetSchema();

        $output = $this->runDbStatus();

        self::assertStringContainsString('Status:   connected', $output);
        self::assertStringContainsString('Migrations: not initialized', $output);
        self::assertNull($this->toRegclass('schema_migrations'));
    }

    public function testDbStatusReportsPendingMigrationCountWithoutApplyingThem(): void
    {
        $this->resetSchema();
        $this->pdo->exec(<<<'SQL'
CREATE TABLE schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
)
SQL);

        $output = $this->runDbStatus();

        self::assertStringContainsString('Migrations: 16 pending', $output);
        self::assertSame([], $this->pdo->query('SELECT version FROM schema_migrations')->fetchAll(PDO::FETCH_COLUMN));
    }

    private function runDbStatus(): string
    {
        $stream = fopen('php://memory', 'w+');
        self::assertIsResource($stream);

        $exitCode = (new DbStatusCommand(
            $this->config,
            dirname(__DIR__, 3) . '/database/migrations'
        ))->run($stream);

        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertSame(0, $exitCode);
        self::assertIsString($output);

        return $output;
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

    private function toRegclass(string $table): ?string
    {
        $statement = $this->pdo->prepare('SELECT to_regclass(:table)');
        $statement->execute(['table' => $table]);
        $result = $statement->fetchColumn();

        return is_string($result) ? $result : null;
    }
}
