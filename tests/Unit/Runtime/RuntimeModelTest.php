<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Runtime;

use FluxQ\Runtime\RuntimeConnection;
use FluxQ\Runtime\RuntimeConsumer;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class RuntimeModelTest extends TestCase
{
    public function testRuntimeGeneratedIdsAreValidOpaqueUuids(): void
    {
        $connection = RuntimeConnection::create('test');
        $consumer = RuntimeConsumer::create($connection->id, '/', 'orders', 'worker-a');

        self::assertMatchesRegularExpression('/^[0-9a-f-]{36}$/', $connection->id);
        self::assertMatchesRegularExpression('/^[0-9a-f-]{36}$/', $consumer->id);
        self::assertSame($connection->id, $consumer->connectionId);
    }

    public function testConnectionRejectsProtocolSpecificEmptyProtocolAndListMetadata(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Connection protocol must not be empty.');

        RuntimeConnection::create('');
    }

    public function testConsumerRejectsEmptyDestination(): void
    {
        $connection = RuntimeConnection::create('test');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Consumer destination must not be empty.');

        RuntimeConsumer::create($connection->id, '/', '', 'worker-a');
    }
}
