<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

final class ConsumerRegistry
{
    /**
     * @var array<string, RuntimeConsumer>
     */
    private array $consumers = [];

    /**
     * @var array<string, true>
     */
    private array $consumedDestinations = [];

    public function add(RuntimeConsumer $consumer): void
    {
        if (isset($this->consumers[$consumer->id])) {
            throw new RuntimeRegistrationException(sprintf(
                'Runtime consumer "%s" is already registered.',
                $consumer->id
            ));
        }

        $this->consumers[$consumer->id] = $consumer;
        $this->consumedDestinations[$this->destinationKey($consumer->virtualHost, $consumer->destination)] = true;
    }

    public function remove(string $consumerId): bool
    {
        if (!isset($this->consumers[$consumerId])) {
            return false;
        }

        unset($this->consumers[$consumerId]);

        return true;
    }

    public function removeByConnection(string $connectionId): int
    {
        $removed = 0;

        foreach ($this->consumers as $id => $consumer) {
            if ($consumer->connectionId !== $connectionId) {
                continue;
            }

            unset($this->consumers[$id]);
            $removed++;
        }

        return $removed;
    }

    public function find(string $consumerId): ?RuntimeConsumer
    {
        return $this->consumers[$consumerId] ?? null;
    }

    /**
     * @return list<RuntimeConsumer>
     */
    public function all(): array
    {
        return array_values($this->consumers);
    }

    /**
     * @return list<RuntimeConsumer>
     */
    public function allByConnection(string $connectionId): array
    {
        return array_values(array_filter(
            $this->consumers,
            static fn (RuntimeConsumer $consumer): bool => $consumer->connectionId === $connectionId
        ));
    }

    public function count(): int
    {
        return count($this->consumers);
    }

    public function countByDestination(string $virtualHost, string $destination): int
    {
        $count = 0;

        foreach ($this->consumers as $consumer) {
            if ($consumer->virtualHost === $virtualHost && $consumer->destination === $destination) {
                $count++;
            }
        }

        return $count;
    }

    public function hasExclusiveConsumer(string $virtualHost, string $destination): bool
    {
        foreach ($this->consumers as $consumer) {
            if (
                $consumer->virtualHost === $virtualHost
                && $consumer->destination === $destination
                && $consumer->exclusive
            ) {
                return true;
            }
        }

        return false;
    }

    public function canRegisterConsumer(string $virtualHost, string $destination, bool $exclusive): bool
    {
        $consumerCount = $this->countByDestination($virtualHost, $destination);

        if ($exclusive) {
            return $consumerCount === 0;
        }

        return !$this->hasExclusiveConsumer($virtualHost, $destination);
    }

    public function hasHadConsumer(string $virtualHost, string $destination): bool
    {
        return isset($this->consumedDestinations[$this->destinationKey($virtualHost, $destination)]);
    }

    public function clear(): void
    {
        $this->consumers = [];
        $this->consumedDestinations = [];
    }

    private function destinationKey(string $virtualHost, string $destination): string
    {
        return $virtualHost . "\0" . $destination;
    }
}
