<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Persistence\Postgres\MigrationFailure;
use FluxQ\Persistence\Postgres\Migrator;
use Throwable;

final readonly class MigrateCommand
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
        $this->write($output, "FluxQ Database Migrations\n\n");

        try {
            $connection = Connection::fromConfig($this->config);

            $this->write($output, sprintf("Database: %s\n", $this->config->database));
            $this->write($output, sprintf(
                "Host:     %s:%d\n\n",
                $this->config->host,
                $this->config->port
            ));

            $result = (new Migrator($connection, $this->migrationDirectory))->migrate();
        } catch (MigrationFailure $exception) {
            $this->write($output, sprintf("Migration failed: %s\n\n", $exception->migration));
            $this->write($output, sprintf("ERROR: %s\n", $this->safeError($exception->getPrevious())));

            return 1;
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $this->safeError($exception)));

            return 1;
        }

        if ($result->count() === 0) {
            $this->write($output, "Nothing to migrate.\n");

            return 0;
        }

        foreach ($result->applied as $migration) {
            $this->write($output, sprintf("Applying %s ... DONE\n", $migration));
        }

        $this->write($output, sprintf("\n%d migrations applied.\n", $result->count()));

        return 0;
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }

    private function safeError(?Throwable $exception): string
    {
        if ($exception === null) {
            return 'Unknown migration error.';
        }

        return $this->config->redact($exception->getMessage());
    }
}
