<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Runtime;

use DateTimeImmutable;
use FluxQ\Broker\Broker;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\PublishTransaction;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use FluxQ\Runtime\BrokerRuntime;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Runtime\ConsumerRegistry;
use FluxQ\Runtime\RuntimeConnection;
use FluxQ\Runtime\RuntimeConsumer;
use FluxQ\Runtime\RuntimeDiagnosticsServer;
use FluxQ\Runtime\RuntimeDrainingComponent;
use FluxQ\Runtime\RuntimeState;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class BrokerRuntimeTest extends TestCase
{
    public function testRuntimeRunsFiniteIdleLoopAndStopsCleanly(): void
    {
        $runtime = $this->runtime();

        $runtime->run(maxIterations: 3);

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertSame(3, $runtime->tickCount());
    }

    public function testShutdownRequestFromIdleSleepIsHonored(): void
    {
        $runtime = null;
        $runtime = $this->runtime(static function () use (&$runtime): void {
            self::assertSame(RuntimeState::Running, $runtime?->state());
            $runtime?->requestShutdown();
        });

        $runtime->run();

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertSame(1, $runtime->tickCount());
    }

    public function testRuntimeCannotStartTwice(): void
    {
        $runtime = $this->runtime();
        $runtime->run(maxIterations: 0);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Broker runtime can only be started once.');

        $runtime->run(maxIterations: 0);
    }

    public function testStoppedRuntimeDoesNotContinueTicking(): void
    {
        $runtime = $this->runtime();

        $runtime->run(maxIterations: 1);
        $runtime->tick();

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertSame(1, $runtime->tickCount());
    }

    public function testStopClearsRuntimeRegistries(): void
    {
        $connections = new ConnectionRegistry();
        $consumers = new ConsumerRegistry();
        $connection = new RuntimeConnection(
            '00000000-0000-4000-8000-000000000201',
            'test',
            new DateTimeImmutable()
        );
        $consumer = new RuntimeConsumer(
            '00000000-0000-4000-8000-000000000202',
            $connection->id,
            '/',
            'orders',
            'worker-a',
            new DateTimeImmutable()
        );
        $connections->add($connection);
        $consumers->add($consumer);

        $runtime = new BrokerRuntime($this->broker(), $connections, $consumers, 0, static function (): void {
        });
        $runtime->run(maxIterations: 0);

        self::assertSame(0, $connections->count());
        self::assertSame(0, $consumers->count());
    }

    public function testRuntimeStartsTicksAndStopsComponents(): void
    {
        $events = [];
        $runtime = new BrokerRuntime(
            $this->broker(),
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            0,
            static function (): void {
            },
            [new RecordingRuntimeComponent($events)]
        );

        $runtime->run(maxIterations: 2);

        self::assertSame(['start', 'tick', 'tick', 'stop'], $events);
    }

    public function testRuntimeShutdownClosesDiagnosticsComponent(): void
    {
        $diagnostics = new RuntimeDiagnosticsServer(new ConnectionRegistry(), new ConsumerRegistry(), port: 0);
        $runtime = new BrokerRuntime(
            $this->broker(),
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            0,
            static function (): void {
            },
            [$diagnostics]
        );

        $runtime->run(maxIterations: 0);

        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $diagnostics->port()), $errorCode, $errorMessage, 0.1);
        if (is_resource($client)) {
            fclose($client);
        }

        self::assertFalse($client);
    }

    public function testShutdownTransitionsThroughDrainingAndCompletesWhenInflightSettles(): void
    {
        $events = [];
        $component = new RecordingDrainingComponent($events, 1);
        $runtime = new BrokerRuntime(
            $this->broker(),
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            0,
            static function (): void {
            },
            [$component],
            drainTimeoutSeconds: 30,
            clock: static fn (): int => 0
        );

        $runtime->run(maxIterations: 1);

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertSame(['start', 'tick', 'begin-drain', 'tick', 'stop'], $events);
    }

    public function testDrainTimeoutForcesFinalCleanup(): void
    {
        $now = 0;
        $events = [];
        $component = new RecordingDrainingComponent($events, 1, settleOnTick: false);
        $runtime = new BrokerRuntime(
            $this->broker(),
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            0,
            static function () use (&$now): void {
                $now += 2_000_000_000;
            },
            [$component],
            drainTimeoutSeconds: 1,
            clock: static function () use (&$now): int {
                return $now;
            }
        );

        $runtime->run(maxIterations: 1);

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertContains('begin-drain', $events);
        self::assertSame(1, $component->inFlightCount());
    }

    public function testRepeatedShutdownRequestsAndStopAreIdempotent(): void
    {
        $events = [];
        $runtime = new BrokerRuntime(
            $this->broker(),
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            0,
            static function (): void {
            },
            [new RecordingRuntimeComponent($events)]
        );

        $runtime->requestShutdown();
        $runtime->requestShutdown();
        $runtime->run(maxIterations: 0);
        $runtime->stop();

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertSame(['start', 'stop'], $events);
    }

    public function testImmediateShutdownWithZeroDrainTimeout(): void
    {
        $events = [];
        $runtime = new BrokerRuntime(
            $this->broker(),
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            0,
            static function (): void {
            },
            [new RecordingDrainingComponent($events, 1, settleOnTick: false)],
            drainTimeoutSeconds: 0
        );

        $runtime->run(maxIterations: 1);

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertSame(['start', 'tick', 'begin-drain', 'stop'], $events);
    }

    private function runtime(?callable $sleep = null): BrokerRuntime
    {
        return new BrokerRuntime(
            $this->broker(),
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            0,
            $sleep ?? static function (): void {
            }
        );
    }

    private function broker(): Broker
    {
        $connection = new Connection(new ConnectionConfig('127.0.0.1', 5432, 'fluxq_test', 'fluxq', null));

        return new Broker(
            new VirtualHostRepository($connection),
            new PublishTransaction($connection),
            new DestinationRepository($connection),
            new SubscriptionRepository($connection),
            new DeliveryRepository($connection)
        );
    }
}

final class RecordingRuntimeComponent implements \FluxQ\Runtime\RuntimeComponent
{
    /**
     * @var list<string>
     */
    private array $events;

    /**
     * @param list<string> $events
     */
    public function __construct(array &$events)
    {
        $this->events = &$events;
    }

    public function start(): void
    {
        $this->events[] = 'start';
    }

    public function tick(): void
    {
        $this->events[] = 'tick';
    }

    public function stop(): void
    {
        $this->events[] = 'stop';
    }
}

final class RecordingDrainingComponent implements RuntimeDrainingComponent
{
    /**
     * @var list<string>
     */
    private array $events;

    public function __construct(
        array &$events,
        private int $inFlight,
        private readonly bool $settleOnTick = true
    ) {
        $this->events = &$events;
    }

    public function start(): void
    {
        $this->events[] = 'start';
    }

    public function tick(): void
    {
        $this->events[] = 'tick';
        if ($this->settleOnTick && in_array('begin-drain', $this->events, true)) {
            $this->inFlight = 0;
        }
    }

    public function stop(): void
    {
        $this->events[] = 'stop';
    }

    public function beginDrain(): void
    {
        $this->events[] = 'begin-drain';
    }

    public function inFlightCount(): int
    {
        return $this->inFlight;
    }
}
