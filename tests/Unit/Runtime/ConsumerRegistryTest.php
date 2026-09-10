<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Runtime;

use DateTimeImmutable;
use FluxQ\Runtime\ConsumerRegistry;
use FluxQ\Runtime\RuntimeConsumer;
use FluxQ\Runtime\RuntimeRegistrationException;
use PHPUnit\Framework\TestCase;

final class ConsumerRegistryTest extends TestCase
{
    public function testAddFindCountListAndListByConnection(): void
    {
        $registry = new ConsumerRegistry();
        $consumerA = $this->consumer(
            '00000000-0000-4000-8000-000000000011',
            '00000000-0000-4000-8000-000000000101'
        );
        $consumerB = $this->consumer(
            '00000000-0000-4000-8000-000000000012',
            '00000000-0000-4000-8000-000000000101'
        );
        $consumerC = $this->consumer(
            '00000000-0000-4000-8000-000000000013',
            '00000000-0000-4000-8000-000000000102'
        );

        $registry->add($consumerA);
        $registry->add($consumerB);
        $registry->add($consumerC);

        self::assertSame($consumerA, $registry->find($consumerA->id));
        self::assertSame(3, $registry->count());
        self::assertSame([$consumerA, $consumerB, $consumerC], $registry->all());
        self::assertSame([$consumerA, $consumerB], $registry->allByConnection($consumerA->connectionId));
    }

    public function testDuplicateConsumerIdFailsClearly(): void
    {
        $registry = new ConsumerRegistry();
        $consumer = $this->consumer(
            '00000000-0000-4000-8000-000000000014',
            '00000000-0000-4000-8000-000000000103'
        );

        $registry->add($consumer);

        $this->expectException(RuntimeRegistrationException::class);
        $this->expectExceptionMessage('Runtime consumer "00000000-0000-4000-8000-000000000014" is already registered.');

        $registry->add($consumer);
    }

    public function testRemoveUnknownRemoveConnectionCleanupAndClear(): void
    {
        $registry = new ConsumerRegistry();
        $connectionId = '00000000-0000-4000-8000-000000000104';
        $consumerA = $this->consumer('00000000-0000-4000-8000-000000000015', $connectionId);
        $consumerB = $this->consumer('00000000-0000-4000-8000-000000000016', $connectionId);
        $consumerC = $this->consumer(
            '00000000-0000-4000-8000-000000000017',
            '00000000-0000-4000-8000-000000000105'
        );

        $registry->add($consumerA);
        $registry->add($consumerB);
        $registry->add($consumerC);

        self::assertFalse($registry->remove('00000000-0000-4000-8000-000000009999'));
        self::assertTrue($registry->remove($consumerA->id));
        self::assertSame(1, $registry->removeByConnection($connectionId));
        self::assertSame([$consumerC], $registry->all());

        $registry->clear();

        self::assertSame(0, $registry->count());
    }

    public function testDestinationConsumerHistorySurvivesFinalConsumerRemovalUntilClear(): void
    {
        $registry = new ConsumerRegistry();
        $consumer = $this->consumer(
            '00000000-0000-4000-8000-000000000018',
            '00000000-0000-4000-8000-000000000106'
        );

        self::assertFalse($registry->hasHadConsumer('/', 'orders'));

        $registry->add($consumer);
        self::assertTrue($registry->hasHadConsumer('/', 'orders'));

        $registry->remove($consumer->id);
        self::assertSame(0, $registry->countByDestination('/', 'orders'));
        self::assertTrue($registry->hasHadConsumer('/', 'orders'));

        $registry->clear();
        self::assertFalse($registry->hasHadConsumer('/', 'orders'));
    }

    public function testExclusiveConsumerRegistrationRulesAreQueueWideAndRuntimeOnly(): void
    {
        $registry = new ConsumerRegistry();
        $exclusive = $this->consumer(
            '00000000-0000-4000-8000-000000000019',
            '00000000-0000-4000-8000-000000000107',
            destination: 'orders',
            exclusive: true
        );
        $unrelated = $this->consumer(
            '00000000-0000-4000-8000-000000000020',
            '00000000-0000-4000-8000-000000000108',
            destination: 'invoices'
        );

        self::assertTrue($registry->canRegisterConsumer('/', 'orders', true));
        self::assertTrue($registry->canRegisterConsumer('/', 'orders', false));

        $registry->add($exclusive);

        self::assertTrue($registry->hasExclusiveConsumer('/', 'orders'));
        self::assertFalse($registry->canRegisterConsumer('/', 'orders', false));
        self::assertFalse($registry->canRegisterConsumer('/', 'orders', true));
        self::assertTrue($registry->canRegisterConsumer('/', 'invoices', false));

        $registry->add($unrelated);
        self::assertSame(2, $registry->count());

        self::assertTrue($registry->remove($exclusive->id));
        self::assertFalse($registry->hasExclusiveConsumer('/', 'orders'));
        self::assertTrue($registry->canRegisterConsumer('/', 'orders', false));
        self::assertTrue($registry->canRegisterConsumer('/', 'orders', true));
        self::assertSame(1, $registry->count());
    }

    public function testExclusiveConsumerCannotRegisterAfterNormalConsumer(): void
    {
        $registry = new ConsumerRegistry();
        $consumer = $this->consumer(
            '00000000-0000-4000-8000-000000000021',
            '00000000-0000-4000-8000-000000000109'
        );

        $registry->add($consumer);

        self::assertTrue($registry->canRegisterConsumer('/', 'orders', false));
        self::assertFalse($registry->canRegisterConsumer('/', 'orders', true));
    }

    private function consumer(
        string $id,
        string $connectionId,
        string $destination = 'orders',
        bool $exclusive = false
    ): RuntimeConsumer
    {
        return new RuntimeConsumer(
            $id,
            $connectionId,
            '/',
            $destination,
            'worker-a',
            new DateTimeImmutable(),
            $exclusive
        );
    }
}
