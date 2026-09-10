<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Persistence\Postgres;

use FluxQ\Persistence\Postgres\ConnectionConfig;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ConnectionConfigTest extends TestCase
{
    public function testItBuildsPostgreSQLDsnWithoutPassword(): void
    {
        $config = new ConnectionConfig('127.0.0.1', 5432, 'fluxq', 'fluxq', 'secret');

        self::assertSame('pgsql:host=127.0.0.1;port=5432;dbname=fluxq', $config->dsn());
    }

    public function testItRedactsPasswordFromMessages(): void
    {
        $config = new ConnectionConfig('127.0.0.1', 5432, 'fluxq', 'fluxq', 'secret');

        self::assertSame('password=[redacted]', $config->redact('password=secret'));
    }

    public function testItFailsClearlyForMissingDatabaseName(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('FLUXQ_DB_NAME is not configured.');

        new ConnectionConfig('127.0.0.1', 5432, '', 'fluxq', null);
    }
}
