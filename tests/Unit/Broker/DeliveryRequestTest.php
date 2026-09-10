<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Broker;

use FluxQ\Broker\AcknowledgeRequest;
use FluxQ\Broker\RejectRequest;
use FluxQ\Broker\ReleaseRequest;
use FluxQ\Broker\ReserveRequest;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class DeliveryRequestTest extends TestCase
{
    public function testReserveRequestKeepsOpaqueRuntimeCorrelationValues(): void
    {
        $request = new ReserveRequest('/', 'orders', 'worker-a', 'consumer-1', 'tag-1');

        self::assertSame('/', $request->virtualHost);
        self::assertSame('orders', $request->destination);
        self::assertSame('worker-a', $request->subscription);
        self::assertSame('consumer-1', $request->consumerId);
        self::assertSame('tag-1', $request->deliveryTag);
    }

    public function testReserveRequestRejectsEmptyNames(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Destination must not be empty.');

        new ReserveRequest('/', '', 'worker-a', 'consumer-1');
    }

    public function testAcknowledgeRejectAndReleaseRequirePositiveDeliveryId(): void
    {
        foreach ([AcknowledgeRequest::class, RejectRequest::class, ReleaseRequest::class] as $requestClass) {
            try {
                new $requestClass(0);
                self::fail(sprintf('%s should reject non-positive delivery IDs.', $requestClass));
            } catch (InvalidArgumentException $exception) {
                self::assertSame('Delivery ID must be positive.', $exception->getMessage());
            }
        }
    }
}
