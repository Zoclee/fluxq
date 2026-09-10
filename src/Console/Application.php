<?php

declare(strict_types=1);

namespace FluxQ\Console;

use FluxQ\Broker\ResourceLimits;
use FluxQ\Console\Commands\DbStatusCommand;
use FluxQ\Console\Commands\BindingListCommand;
use FluxQ\Console\Commands\BrokerStatsCommand;
use FluxQ\Console\Commands\ConnectionListCommand;
use FluxQ\Console\Commands\ConsumerListCommand;
use FluxQ\Console\Commands\HealthCommand;
use FluxQ\Console\Commands\MigrateCommand;
use FluxQ\Console\Commands\MessagePeekCommand;
use FluxQ\Console\Commands\QueueListCommand;
use FluxQ\Console\Commands\QueueShowCommand;
use FluxQ\Console\Commands\ReadinessCommand;
use FluxQ\Console\Commands\ReadOnlyDatabaseContext;
use FluxQ\Console\Commands\ServerStartCommand;
use FluxQ\Console\Commands\SubscriptionListCommand;
use FluxQ\Console\Commands\UserCreateCommand;
use FluxQ\Console\Commands\UserClearPermissionsCommand;
use FluxQ\Console\Commands\UserGrantVhostCommand;
use FluxQ\Console\Commands\UserListCommand;
use FluxQ\Console\Commands\UserListPermissionsCommand;
use FluxQ\Console\Commands\UserListVhostsCommand;
use FluxQ\Console\Commands\UserSetPermissionsCommand;
use FluxQ\Console\Commands\VhostCreateCommand;
use FluxQ\Console\Commands\VhostListCommand;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Runtime\RuntimeDiagnosticsClient;
use FluxQ\Support\Dotenv;
use Throwable;

final class Application
{
    public const VERSION = '0.2.0';

    public function __construct(
        private readonly ?string $projectRoot = null
    ) {
    }

    /**
     * @param list<string> $argv
     * @param resource|null $output
     */
    public function run(array $argv, mixed $output = null): int
    {
        $output ??= STDOUT;

        $command = $argv[1] ?? 'help';

        return match ($command) {
            'help', '-h', '--help' => $this->showHelp($output),
            '--version', '-V' => $this->showVersion($output),
            'db:status' => $this->dbStatus($output),
            'migrate' => $this->migrate($output),
            'health' => $this->health($output),
            'readiness' => $this->readiness($output),
            'server:start' => $this->serverStart($output),
            'connection:list' => $this->runtimeCommand(ConnectionListCommand::class, $output),
            'consumer:list' => $this->runtimeCommand(ConsumerListCommand::class, $output),
            'broker:stats' => $this->brokerStats($output),
            'user:create' => $this->userCreate(array_slice($argv, 2), $output),
            'user:list' => $this->userList($output),
            'user:grant-vhost' => $this->userGrantVhost(array_slice($argv, 2), $output),
            'user:list-vhosts' => $this->userListVhosts(array_slice($argv, 2), $output),
            'user:set-permissions' => $this->userSetPermissions(array_slice($argv, 2), $output),
            'user:list-permissions' => $this->userListPermissions(array_slice($argv, 2), $output),
            'user:clear-permissions' => $this->userClearPermissions(array_slice($argv, 2), $output),
            'vhost:create' => $this->vhostCreate(array_slice($argv, 2), $output),
            'vhost:list' => $this->readOnlyCommand(VhostListCommand::class, [], $output),
            'queue:list' => $this->readOnlyCommand(QueueListCommand::class, [], $output),
            'queue:show' => $this->readOnlyCommand(QueueShowCommand::class, array_slice($argv, 2), $output, true),
            'binding:list' => $this->readOnlyCommand(BindingListCommand::class, [], $output),
            'subscription:list' => $this->readOnlyCommand(SubscriptionListCommand::class, [], $output),
            'message:peek' => $this->readOnlyCommand(MessagePeekCommand::class, array_slice($argv, 2), $output, true),
            default => $this->showUnknownCommand($command, $output),
        };
    }

    /**
     * @param resource $output
     */
    private function showHelp(mixed $output): int
    {
        $this->write($output, sprintf(<<<'HELP'
FluxQ %s

Usage:
  fluxq <command>

Available commands:
  help                  Display this help message
  --version             Display the FluxQ version

Database:
  db:status             Show PostgreSQL connection and migration status
  migrate               Apply pending PostgreSQL database migrations

Server:
  health                Check local runtime health
  readiness             Check whether FluxQ can accept broker traffic
  server:start          Start the FluxQ broker runtime
  connection:list       List active runtime connections
  consumer:list         List active runtime consumers

Broker state:
  broker:stats          Show runtime and persistence statistics
  user:create <user>    Create a broker user
  user:list             List broker users
  user:grant-vhost      Grant a user access to a virtual host
  user:list-vhosts <username>    List virtual-host grants
  user:set-permissions  Set user permissions for a virtual host
  user:list-permissions List user permissions
  user:clear-permissions Clear user permissions for a virtual host
  vhost:create <name>    Create a virtual host
  vhost:list            List virtual hosts
  queue:list            List queues
  queue:show <queue>    Show queue details
  binding:list          List bindings
  subscription:list     List subscriptions
  message:peek <queue>  Inspect queued messages

HELP, self::VERSION));

        return 0;
    }

    /**
     * @param resource $output
     */
    private function showVersion(mixed $output): int
    {
        $this->write($output, sprintf("FluxQ %s\n", self::VERSION));

        return 0;
    }

    /**
     * @param resource $output
     */
    private function showUnknownCommand(string $command, mixed $output): int
    {
        $this->write($output, sprintf("Unknown command: %s\n\n", $command));
        $this->showHelp($output);

        return 1;
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }

    /**
     * @param resource $output
     */
    private function migrate(mixed $output): int
    {
        try {
            [$projectRoot, $config] = $this->projectContext();
            $databaseConfig = ConnectionConfig::fromArray($config['database']);
        } catch (Throwable $exception) {
            $this->write($output, "FluxQ Database Migrations\n\n");
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new MigrateCommand(
            $databaseConfig,
            $projectRoot . '/database/migrations'
        ))->run($output);
    }

    /**
     * @param resource $output
     */
    private function dbStatus(mixed $output): int
    {
        try {
            [$projectRoot, $config] = $this->projectContext();
            $databaseConfig = ConnectionConfig::fromArray($config['database']);
        } catch (Throwable $exception) {
            $this->write($output, "FluxQ Database Status\n\n");
            $this->write($output, "Status:   disconnected\n\n");
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new DbStatusCommand(
            $databaseConfig,
            $projectRoot . '/database/migrations'
        ))->run($output);
    }

    /**
     * @param resource $output
     */
    private function serverStart(mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $databaseConfig = ConnectionConfig::fromArray($config['database']);
            $amqpConfig = $config['amqp'] ?? [];
            $amqpTlsConfig = $amqpConfig['tls'] ?? [];
            $limits = ResourceLimits::fromArray($config['limits'] ?? []);
            $diagnosticsConfig = $config['diagnostics'] ?? [];
            $shutdownConfig = $config['shutdown'] ?? [];
        } catch (Throwable $exception) {
            $this->write($output, "FluxQ Message Broker\n\n");
            $this->write($output, "Status:   stopped\n\n");
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new ServerStartCommand(
            $databaseConfig,
            self::VERSION,
            (string) ($amqpConfig['host'] ?? '127.0.0.1'),
            (int) ($amqpConfig['port'] ?? 5672),
            (int) ($amqpConfig['heartbeat'] ?? 60),
            (string) ($diagnosticsConfig['host'] ?? '127.0.0.1'),
            (int) ($diagnosticsConfig['port'] ?? 5673),
            limits: $limits,
            amqpEnabled: (bool) ($amqpConfig['enabled'] ?? true),
            amqpTlsEnabled: (bool) ($amqpTlsConfig['enabled'] ?? false),
            amqpTlsHost: (string) ($amqpTlsConfig['host'] ?? '0.0.0.0'),
            amqpTlsPort: (int) ($amqpTlsConfig['port'] ?? 5671),
            amqpTlsCert: isset($amqpTlsConfig['cert']) ? (string) $amqpTlsConfig['cert'] : null,
            amqpTlsKey: isset($amqpTlsConfig['key']) ? (string) $amqpTlsConfig['key'] : null,
            amqpTlsCa: isset($amqpTlsConfig['ca']) && $amqpTlsConfig['ca'] !== '' ? (string) $amqpTlsConfig['ca'] : null,
            drainTimeoutSeconds: (int) ($shutdownConfig['drain_timeout'] ?? 30)
        ))->run($output);
    }

    /**
     * @param resource $output
     */
    private function health(mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $diagnosticsConfig = $config['diagnostics'] ?? [];
        } catch (Throwable $exception) {
            $this->write($output, "FluxQ Health\n\n");
            $this->write($output, "Runtime: unavailable\n");
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new HealthCommand($this->diagnosticsClient($diagnosticsConfig)))->run($output);
    }

    /**
     * @param resource $output
     */
    private function readiness(mixed $output): int
    {
        try {
            [$projectRoot, $config] = $this->projectContext();
            $databaseConfig = ConnectionConfig::fromArray($config['database']);
            $diagnosticsConfig = $config['diagnostics'] ?? [];
            $amqpConfig = $config['amqp'] ?? [];
            $amqpTlsConfig = is_array($amqpConfig['tls'] ?? null) ? $amqpConfig['tls'] : [];
        } catch (Throwable $exception) {
            $this->write($output, "FluxQ Readiness\n\n");
            $this->write($output, "Ready: no\n");
            $this->write($output, sprintf("Reason: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new ReadinessCommand(
            $this->diagnosticsClient($diagnosticsConfig),
            $databaseConfig,
            $projectRoot . '/database/migrations',
            (bool) ($amqpConfig['enabled'] ?? true),
            (bool) ($amqpTlsConfig['enabled'] ?? false)
        ))->run($output);
    }

    /**
     * @param class-string $commandClass
     * @param resource $output
     */
    private function runtimeCommand(string $commandClass, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $diagnosticsConfig = $config['diagnostics'] ?? [];
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        $command = new $commandClass($this->diagnosticsClient($diagnosticsConfig));

        return $command->run($output);
    }

    /**
     * @param resource $output
     */
    private function brokerStats(mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $context = new ReadOnlyDatabaseContext(ConnectionConfig::fromArray($config['database']));
            $diagnosticsConfig = $config['diagnostics'] ?? [];
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new BrokerStatsCommand($context, $this->diagnosticsClient($diagnosticsConfig)))->run($output);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    private function userCreate(array $arguments, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new UserCreateCommand($connection))->run($arguments, $output);
    }

    /**
     * @param resource $output
     */
    private function userList(mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new UserListCommand($connection))->run($output);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    private function userGrantVhost(array $arguments, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new UserGrantVhostCommand($connection))->run($arguments, $output);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    private function userListVhosts(array $arguments, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new UserListVhostsCommand($connection))->run($arguments, $output);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    private function userSetPermissions(array $arguments, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new UserSetPermissionsCommand($connection))->run($arguments, $output);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    private function userListPermissions(array $arguments, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new UserListPermissionsCommand($connection))->run($arguments, $output);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    private function userClearPermissions(array $arguments, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new UserClearPermissionsCommand($connection))->run($arguments, $output);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    private function vhostCreate(array $arguments, mixed $output): int
    {
        try {
            [, $config] = $this->projectContext();
            $connection = Connection::fromConfig(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        return (new VhostCreateCommand($connection))->run($arguments, $output);
    }

    /**
     * @param class-string $commandClass
     * @param list<string> $arguments
     * @param resource $output
     */
    private function readOnlyCommand(string $commandClass, array $arguments, mixed $output, bool $passesArguments = false): int
    {
        try {
            [, $config] = $this->projectContext();
            $context = new ReadOnlyDatabaseContext(ConnectionConfig::fromArray($config['database']));
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        $command = new $commandClass($context);

        if (!$passesArguments) {
            return $command->run($output);
        }

        return $command->run($arguments, $output);
    }

    /**
     * @param array<string, mixed> $diagnosticsConfig
     */
    private function diagnosticsClient(array $diagnosticsConfig): RuntimeDiagnosticsClient
    {
        return new RuntimeDiagnosticsClient(
            (string) ($diagnosticsConfig['host'] ?? '127.0.0.1'),
            (int) ($diagnosticsConfig['port'] ?? 5673)
        );
    }

    /**
     * @return array{0: string, 1: array{database: array<string, mixed>, amqp?: array<string, mixed>}}
     */
    private function projectContext(): array
    {
        $projectRoot = $this->projectRoot ?? dirname(__DIR__, 2);
        Dotenv::load($projectRoot . '/.env');
        $config = require $projectRoot . '/config/fluxq.php';

        return [$projectRoot, $config];
    }
}
