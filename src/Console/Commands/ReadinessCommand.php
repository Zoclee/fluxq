<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Runtime\RuntimeDiagnostics;
use RuntimeException;
use Throwable;

final readonly class ReadinessCommand
{
    public function __construct(
        private RuntimeDiagnostics $diagnostics,
        private ConnectionConfig $databaseConfig,
        private string $migrationDirectory,
        private bool $amqpEnabled = true,
        private bool $amqpTlsEnabled = false
    ) {
    }

    /**
     * @param resource $output
     */
    public function run(mixed $output): int
    {
        $this->write($output, "FluxQ Readiness\n\n");

        try {
            $stats = $this->diagnostics->stats();
        } catch (RuntimeException) {
            return $this->notReady($output, 'runtime unavailable');
        }

        $state = (string) ($stats['state'] ?? 'unknown');
        if ($state !== 'running') {
            return $this->notReady($output, sprintf('runtime is %s', self::reasonState($state)));
        }

        try {
            $connection = Connection::fromConfig($this->databaseConfig);
            $pendingMigrations = (new Migrator($connection, $this->migrationDirectory))->pendingMigrationCount();
        } catch (Throwable $exception) {
            return $this->notReady($output, sprintf('database unavailable: %s', $this->databaseConfig->redact($exception->getMessage())));
        }

        if ($pendingMigrations !== 0) {
            $reason = $pendingMigrations === null ? 'migrations are not initialized' : sprintf('%d migrations pending', $pendingMigrations);

            return $this->notReady($output, $reason);
        }

        $listeners = is_array($stats['listeners'] ?? null) ? $stats['listeners'] : [];
        if ($this->amqpEnabled && !self::listenerIsRunning($listeners, 'amqp')) {
            return $this->notReady($output, 'AMQP listener is not running');
        }

        if ($this->amqpTlsEnabled && !self::listenerIsRunning($listeners, 'amqp_tls')) {
            return $this->notReady($output, 'AMQP TLS listener is not running');
        }

        $this->write($output, "Ready: yes\n");
        $this->write($output, "Runtime: Running\n");
        $this->write($output, "Database: connected\n");
        $this->write($output, "Migrations: up to date\n");

        return 0;
    }

    /**
     * @param resource $output
     */
    private function notReady(mixed $output, string $reason): int
    {
        $this->write($output, "Ready: no\n");
        $this->write($output, sprintf("Reason: %s\n", $reason));

        return 1;
    }

    /**
     * @param array<string, mixed> $listeners
     */
    private static function listenerIsRunning(array $listeners, string $name): bool
    {
        $listener = $listeners[$name] ?? null;

        return is_array($listener) && ($listener['enabled'] ?? null) === true && ($listener['running'] ?? null) === true;
    }

    private static function reasonState(string $state): string
    {
        return match ($state) {
            'created' => 'created',
            'starting' => 'starting',
            'running' => 'running',
            'draining' => 'draining',
            'stopping' => 'stopping',
            'stopped' => 'stopped',
            default => 'unavailable',
        };
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }
}
