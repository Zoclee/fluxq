<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Persistence\Postgres\Migrator;
use PDO;
use Throwable;

final readonly class DbStatusCommand
{
    public function __construct(
        private ConnectionConfig $config,
        private string $migrationDirectory
    ) {
    }

    /**
     * @param resource $output
     */
    public function run(mixed $output): int
    {
        $this->write($output, "FluxQ Database Status\n\n");

        try {
            $connection = Connection::fromConfig($this->config);
            $pdo = $connection->pdo();
            $serverVersion = $this->serverVersion($pdo);
            $pendingMigrations = (new Migrator($connection, $this->migrationDirectory))->pendingMigrationCount();
        } catch (Throwable $exception) {
            $this->write($output, "Status:   disconnected\n\n");
            $this->write($output, sprintf("ERROR: %s\n", $this->config->redact($exception->getMessage())));

            return 1;
        }

        $this->write($output, "Status:   connected\n");
        $this->write($output, "Driver:   PostgreSQL\n");
        $this->write($output, sprintf("Host:     %s\n", $this->config->host));
        $this->write($output, sprintf("Port:     %d\n", $this->config->port));
        $this->write($output, sprintf("Database: %s\n", $this->config->database));
        $this->write($output, sprintf("User:     %s\n", $this->config->user));
        $this->write($output, sprintf("Version:  %s\n\n", $serverVersion));
        $this->write($output, sprintf("Migrations: %s\n", $this->migrationStatus($pendingMigrations)));

        return 0;
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }

    private function serverVersion(PDO $pdo): string
    {
        $statement = $pdo->query('SELECT version()');

        if ($statement === false) {
            return 'unknown';
        }

        $version = $statement->fetchColumn();

        return is_string($version) ? $version : 'unknown';
    }

    private function migrationStatus(?int $pendingMigrations): string
    {
        if ($pendingMigrations === null) {
            return 'not initialized';
        }

        if ($pendingMigrations === 0) {
            return 'up to date';
        }

        return sprintf('%d pending', $pendingMigrations);
    }
}
