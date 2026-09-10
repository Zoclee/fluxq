<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Broker;

use FluxQ\Broker\ResourcePermissionMatcher;
use PHPUnit\Framework\TestCase;

final class ResourcePermissionMatcherTest extends TestCase
{
    public function testRegexResourceMatchingAllowsAndDeniesResources(): void
    {
        self::assertTrue(ResourcePermissionMatcher::matches('^orders\\.', 'orders.created'));
        self::assertFalse(ResourcePermissionMatcher::matches('^orders\\.', 'billing.created'));
        self::assertFalse(ResourcePermissionMatcher::matches('', 'orders.created'));
    }

    public function testRegexValidationRejectsMalformedExpressions(): void
    {
        self::assertTrue(ResourcePermissionMatcher::isValid('^orders$'));
        self::assertFalse(ResourcePermissionMatcher::isValid('['));
    }
}
