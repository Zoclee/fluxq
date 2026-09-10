<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Broker\VirtualHost;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use RuntimeException;
use Throwable;

final readonly class ReadOnlyDatabaseContext
{
    public function __construct(
        private ConnectionConfig $config
    ) {
    }

    public function connect(): Connection
    {
        return Connection::fromConfig($this->config);
    }

    public function defaultVirtualHost(Connection $connection): VirtualHost
    {
        $virtualHost = (new VirtualHostRepository($connection))->findByName('/');

        if ($virtualHost === null) {
            throw new RuntimeException('Default virtual host "/" was not found.');
        }

        return $virtualHost;
    }

    public function safeError(Throwable $exception): string
    {
        return $this->config->redact($exception->getMessage());
    }
}
