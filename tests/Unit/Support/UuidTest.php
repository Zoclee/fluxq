<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Support;

use FluxQ\Support\Uuid;
use PHPUnit\Framework\TestCase;

final class UuidTest extends TestCase
{
    public function testGeneratedUuidHasValidV4Format(): void
    {
        $uuid = Uuid::v4();

        self::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $uuid
        );
    }

    public function testGeneratedUuidHasVersionFourNibble(): void
    {
        self::assertSame('4', Uuid::v4()[14]);
    }

    public function testGeneratedUuidHasRfcVariantBits(): void
    {
        self::assertContains(Uuid::v4()[19], ['8', '9', 'a', 'b']);
    }

    public function testGeneratedUuidsDifferAcrossRepeatedCalls(): void
    {
        $uuids = [];

        for ($i = 0; $i < 20; $i++) {
            $uuids[] = Uuid::v4();
        }

        self::assertSame($uuids, array_unique($uuids));
    }
}
