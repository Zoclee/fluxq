<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

use Closure;
use FluxQ\Broker\Broker;
use RuntimeException;

final class BrokerRuntime
{
    private RuntimeState $state = RuntimeState::Created;
    private bool $shutdownRequested = false;
    private int $tickCount = 0;
    private Closure $sleep;
    private Closure $clock;
    private ?int $drainStartedAt = null;

    /**
     * @param list<RuntimeComponent> $components
     */
    public function __construct(
        private readonly Broker $broker,
        private readonly ConnectionRegistry $connections,
        private readonly ConsumerRegistry $consumers,
        private readonly int $idleIntervalMicroseconds = 50000,
        ?callable $sleep = null,
        private readonly array $components = [],
        private readonly int $drainTimeoutSeconds = 30,
        ?callable $clock = null
    ) {
        if ($this->idleIntervalMicroseconds < 0) {
            throw new RuntimeException('Runtime idle interval must not be negative.');
        }

        if ($this->drainTimeoutSeconds < 0) {
            throw new RuntimeException('Runtime drain timeout must not be negative.');
        }

        $this->sleep = $sleep instanceof Closure ? $sleep : Closure::fromCallable($sleep ?? 'usleep');
        $this->clock = $clock instanceof Closure ? $clock : Closure::fromCallable($clock ?? static fn (): int => hrtime(true));
    }

    public function run(?int $maxIterations = null): void
    {
        if ($this->state !== RuntimeState::Created) {
            throw new RuntimeException('Broker runtime can only be started once.');
        }

        if ($maxIterations !== null && $maxIterations < 0) {
            throw new RuntimeException('Runtime max iterations must not be negative.');
        }

        $this->state = RuntimeState::Starting;
        foreach ($this->components as $component) {
            $component->start();
        }

        $this->state = RuntimeState::Running;
        if ($this->shutdownRequested) {
            $this->requestShutdown();
        }

        try {
            while ($this->state === RuntimeState::Running || $this->state === RuntimeState::Draining) {
                if ($maxIterations !== null && $this->tickCount >= $maxIterations && $this->state === RuntimeState::Running) {
                    $this->requestShutdown();
                    if ($this->state === RuntimeState::Stopping) {
                        break;
                    }
                }

                $this->tick();

                if ($this->state === RuntimeState::Running || $this->state === RuntimeState::Draining) {
                    ($this->sleep)($this->idleIntervalMicroseconds);
                }

                if ($this->state === RuntimeState::Draining && $this->drainIsComplete()) {
                    $this->state = RuntimeState::Stopping;
                }
            }
        } finally {
            $this->stop();
        }
    }

    public function tick(): void
    {
        if ($this->state !== RuntimeState::Running && $this->state !== RuntimeState::Draining) {
            return;
        }

        $this->tickCount++;

        foreach ($this->components as $component) {
            $component->tick();
        }

        if ($this->state === RuntimeState::Draining && $this->drainIsComplete()) {
            $this->state = RuntimeState::Stopping;
        }
    }

    public function requestShutdown(): void
    {
        if ($this->state === RuntimeState::Stopped || $this->state === RuntimeState::Stopping) {
            return;
        }

        $this->shutdownRequested = true;

        if ($this->state === RuntimeState::Running) {
            $this->beginDrain();
            if ($this->state === RuntimeState::Draining && $this->drainIsComplete()) {
                $this->state = RuntimeState::Stopping;
            }
        }
    }

    public function stop(): void
    {
        if ($this->state === RuntimeState::Stopped) {
            return;
        }

        $this->state = RuntimeState::Stopping;

        foreach (array_reverse($this->components) as $component) {
            $component->stop();
        }

        $this->connections->clear();
        $this->consumers->clear();
        $this->state = RuntimeState::Stopped;
    }

    public function state(): RuntimeState
    {
        return $this->state;
    }

    public function tickCount(): int
    {
        return $this->tickCount;
    }

    public function unackedCount(): int
    {
        $count = 0;
        foreach ($this->components as $component) {
            if ($component instanceof RuntimeDrainingComponent) {
                $count += $component->inFlightCount();
            }
        }

        return $count;
    }

    private function beginDrain(): void
    {
        if ($this->state === RuntimeState::Draining) {
            return;
        }

        $this->state = RuntimeState::Draining;
        $this->drainStartedAt = $this->now();

        foreach ($this->components as $component) {
            if ($component instanceof RuntimeDrainingComponent) {
                $component->beginDrain();
            }
        }

        if ($this->drainTimeoutSeconds === 0) {
            $this->state = RuntimeState::Stopping;
        }
    }

    private function drainIsComplete(): bool
    {
        if ($this->unackedCount() === 0) {
            return true;
        }

        return $this->elapsedDrainSeconds() >= $this->drainTimeoutSeconds;
    }

    private function elapsedDrainSeconds(): float
    {
        if ($this->drainStartedAt === null) {
            return 0.0;
        }

        return ($this->now() - $this->drainStartedAt) / 1_000_000_000;
    }

    private function now(): int
    {
        return ($this->clock)();
    }
}
