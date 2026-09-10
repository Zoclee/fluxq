<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Broker;

use FluxQ\Broker\PublishRequest;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class PublishRequestTest extends TestCase
{
    public function testDefaultsAreProtocolNeutralAndStable(): void
    {
        $request = new PublishRequest(
            virtualHost: '/',
            source: 'orders',
            routingKey: 'order.created',
            payload: 'payload'
        );

        self::assertSame('/', $request->virtualHost);
        self::assertSame('orders', $request->source);
        self::assertSame('order.created', $request->routingKey);
        self::assertSame('payload', $request->payload);
        self::assertSame([], $request->headers);
        self::assertNull($request->contentType);
        self::assertNull($request->contentEncoding);
        self::assertSame(0, $request->priority);
        self::assertTrue($request->persistent);
        self::assertNull($request->messageId);
    }

    public function testBinaryPayloadIsPreservedByteForByte(): void
    {
        $payload = "\x00\x01\x7F\x80\xFE\xFFabc\x00def";

        $request = new PublishRequest(
            virtualHost: '/',
            source: 'binary',
            routingKey: '',
            payload: $payload
        );

        self::assertSame($payload, $request->payload);
    }

    public function testEmptyVirtualHostIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Virtual host must not be empty.');

        new PublishRequest('', 'orders', 'order.created', 'payload');
    }

    public function testEmptyRoutingSourceIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Routing source must not be empty.');

        new PublishRequest('/', '', 'order.created', 'payload');
    }

    public function testListHeadersAreRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Message headers must be a JSON object.');

        new PublishRequest('/', 'orders', 'order.created', 'payload', ['not', 'an', 'object']);
    }

    public function testInvalidPriorityIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Message priority must be between 0 and 255.');

        new PublishRequest('/', 'orders', 'order.created', 'payload', priority: 256);
    }

    public function testInvalidMessageIdIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Message ID must be a valid UUID.');

        new PublishRequest('/', 'orders', 'order.created', 'payload', messageId: 'not-a-uuid');
    }
}
