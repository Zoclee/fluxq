<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Console\Commands\ReadinessCommand;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Runtime\RuntimeDiagnostics;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class ReadinessCommandTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;

    #[Before]
    public function setUpDatabase(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');

        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL readiness integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();
        $this->assertSafeTestDatabase();
        $this->resetSchema();

        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();
    }

    public function testReadinessSucceedsWhenRuntimeAndDatabaseAreHealthy(): void
    {
        [$exitCode, $output] = $this->runCommand(new ReadinessCommand(
            new ReadyRuntimeDiagnostics(),
            $this->connection->config(),
            dirname(__DIR__, 3) . '/database/migrations'
        ));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Ready: yes', $output);
        self::assertStringContainsString('Runtime: Running', $output);
        self::assertStringContainsString('Migrations: up to date', $output);
    }

    public function testReadinessFailsWhenMigrationsArePending(): void
    {
        $migrationDirectory = $this->temporaryMigrationDirectory();
        file_put_contents($migrationDirectory . DIRECTORY_SEPARATOR . '99999999999999_pending.sql', "SELECT 1;\n");

        try {
            [$exitCode, $output] = $this->runCommand(new ReadinessCommand(
                new ReadyRuntimeDiagnostics(),
                $this->connection->config(),
                $migrationDirectory
            ));

            self::assertSame(1, $exitCode);
            self::assertStringContainsString('Ready: no', $output);
            self::assertStringContainsString('Reason: 1 migrations pending', $output);
        } finally {
            @unlink($migrationDirectory . DIRECTORY_SEPARATOR . '99999999999999_pending.sql');
            @rmdir($migrationDirectory);
        }
    }

    /**
     * @return array{0: int, 1: string}
     */
    private function runCommand(ReadinessCommand $command): array
    {
        $stream = fopen('php://memory', 'w+');
        self::assertIsResource($stream);

        $exitCode = $command->run($stream);

        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);

        return [$exitCode, $output];
    }

    private function temporaryMigrationDirectory(): string
    {
        $directory = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'fluxq-migrations-' . bin2hex(random_bytes(8));

        if (!mkdir($directory) && !is_dir($directory)) {
            self::fail(sprintf('Could not create temporary migration directory: %s', $directory));
        }

        return $directory;
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function assertSafeTestDatabase(): void
    {
        $databaseName = $this->pdo->query('SELECT current_database()')->fetchColumn();

        if (!is_string($databaseName) || !str_contains($databaseName, 'test')) {
            self::fail(sprintf(
                'Refusing to reset PostgreSQL database "%s"; FLUXQ_TEST_DATABASE_URL must point to a test database.',
                is_scalar($databaseName) ? (string) $databaseName : 'unknown'
            ));
        }
    }
}

final readonly class ReadyRuntimeDiagnostics implements RuntimeDiagnostics
{
    public function stats(): array
    {
        return [
            'state' => 'running',
            'connections' => 0,
            'consumers' => 0,
            'unacked' => 0,
            'listeners' => [
                'amqp' => ['enabled' => true, 'running' => true, 'host' => '127.0.0.1', 'port' => 5672],
                'amqp_tls' => ['enabled' => false, 'running' => false, 'host' => null, 'port' => null],
            ],
        ];
    }

    public function connections(): array
    {
        return [];
    }

    public function consumers(): array
    {
        return [];
    }
}
