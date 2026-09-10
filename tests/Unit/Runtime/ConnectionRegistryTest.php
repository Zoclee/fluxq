<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Runtime;

use DateTimeImmutable;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Runtime\RuntimeConnection;
use FluxQ\Runtime\RuntimeRegistrationException;
use PHPUnit\Framework\TestCase;

final class ConnectionRegistryTest extends TestCase
{
    public function testAddFindCountAndListConnections(): void
    {
        $registry = new ConnectionRegistry();
        $connection = $this->connection('00000000-0000-4000-8000-000000000001');

        $registry->add($connection);

        self::assertSame($connection, $registry->find($connection->id));
        self::assertSame(1, $registry->count());
        self::assertSame([$connection], $registry->all());
    }

    public function testDuplicateConnectionIdFailsClearly(): void
    {
        $registry = new ConnectionRegistry();
        $connection = $this->connection('00000000-0000-4000-8000-000000000002');

        $registry->add($connection);

        $this->expectException(RuntimeRegistrationException::class);
        $this->expectExceptionMessage('Runtime connection "00000000-0000-4000-8000-000000000002" is already registered.');

        $registry->add($connection);
    }

    public function testRemoveUnknownRemoveAndClear(): void
    {
        $registry = new ConnectionRegistry();
        $connection = $this->connection('00000000-0000-4000-8000-000000000003');

        $registry->add($connection);

        self::assertFalse($registry->remove('00000000-0000-4000-8000-000000009999'));
        self::assertTrue($registry->remove($connection->id));
        self::assertNull($registry->find($connection->id));

        $registry->add($connection);
        $registry->clear();

        self::assertSame(0, $registry->count());
        self::assertSame([], $registry->all());
    }

    private function connection(string $id): RuntimeConnection
    {
        return new RuntimeConnection($id, 'test', new DateTimeImmutable(), 'local');
    }
}
