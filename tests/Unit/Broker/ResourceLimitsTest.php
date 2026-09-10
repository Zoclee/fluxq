<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Broker;

use FluxQ\Broker\ResourceLimits;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ResourceLimitsTest extends TestCase
{
    public function testDefaultsMatchConfiguredRuntimeCeilings(): void
    {
        $limits = new ResourceLimits();

        self::assertSame(1000, $limits->maxConnections);
        self::assertSame(256, $limits->maxChannelsPerConnection);
        self::assertSame(256, $limits->maxConsumersPerConnection);
        self::assertSame(64, $limits->maxConsumersPerChannel);
        self::assertSame(1048576, $limits->maxFrameSize);
        self::assertSame(16777216, $limits->maxMessageSize);
        self::assertSame(10000, $limits->maxQueuesPerVirtualHost);
        self::assertSame(1000000, $limits->maxQueueDepth);
    }

    public function testZeroMeansUnlimitedForLimitChecks(): void
    {
        $limits = ResourceLimits::unlimited();

        self::assertTrue($limits->allows(0, 1_000_000, 1_000_000));
        self::assertTrue($limits->allows($limits->maxConnections, 1_000_000));
    }

    public function testNegativeLimitsAreRejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('must be zero or greater');

        new ResourceLimits(maxConnections: -1);
    }
}
