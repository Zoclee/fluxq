<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Broker;

use FluxQ\Broker\TopicMatcher;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TopicMatcherTest extends TestCase
{
    #[DataProvider('matchesProvider')]
    public function testTopicPatternMatching(string $pattern, string $routingKey, bool $expected): void
    {
        self::assertSame($expected, (new TopicMatcher())->matches($pattern, $routingKey));
    }

    /**
     * @return iterable<string, array{0: string, 1: string, 2: bool}>
     */
    public static function matchesProvider(): iterable
    {
        yield 'literal match' => ['stock.us', 'stock.us', true];
        yield 'literal mismatch' => ['stock.eu', 'stock.us', false];
        yield 'star one word' => ['stock.*', 'stock.us', true];
        yield 'star not zero words' => ['stock.*', 'stock', false];
        yield 'star not multiple words' => ['stock.*', 'stock.us.nyse', false];
        yield 'hash zero words at end' => ['stock.#', 'stock', true];
        yield 'hash one word at end' => ['stock.#', 'stock.us', true];
        yield 'hash multiple words at end' => ['stock.#', 'stock.us.nyse', true];
        yield 'hash all empty key' => ['#', '', true];
        yield 'hash all one word' => ['#', 'audit', true];
        yield 'star all requires one word' => ['*', 'audit', true];
        yield 'star all rejects empty key' => ['*', '', false];
        yield 'a star c' => ['a.*.c', 'a.b.c', true];
        yield 'a star c rejects missing middle' => ['a.*.c', 'a.c', false];
        yield 'a star c rejects extra middle' => ['a.*.c', 'a.b.d.c', false];
        yield 'a hash c zero middle' => ['a.#.c', 'a.c', true];
        yield 'a hash c one middle' => ['a.#.c', 'a.b.c', true];
        yield 'a hash c multiple middle' => ['a.#.c', 'a.b.d.c', true];
        yield 'a hash c requires trailing literal' => ['a.#.c', 'a.b.d', false];
        yield 'hash error suffix' => ['#.error', 'error', true];
        yield 'hash error nested' => ['#.error', 'service.database.error', true];
        yield 'hash error rejects suffix mismatch' => ['#.error', 'service.database.warning', false];
        yield 'audit hash' => ['audit.#', 'audit.security.login', true];
        yield 'a two stars' => ['a.*.*', 'a.b.c', true];
        yield 'a two stars rejects one' => ['a.*.*', 'a.b', false];
        yield 'mixed star hash' => ['orders.*.#.failed', 'orders.eu.card.payment.failed', true];
        yield 'multiple hashes deterministic' => ['#.audit.#', 'system.audit.user.created', true];
        yield 'malformed pattern double dot' => ['orders..failed', 'orders.eu.failed', false];
        yield 'malformed routing key double dot' => ['orders.#', 'orders..failed', false];
        yield 'malformed pattern trailing dot' => ['orders.', 'orders', false];
        yield 'malformed routing key trailing dot' => ['orders.#', 'orders.', false];
    }
}
