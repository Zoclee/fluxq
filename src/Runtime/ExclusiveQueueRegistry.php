<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

final class ExclusiveQueueRegistry
{
    /**
     * @var array<string, string>
     */
    private array $owners = [];

    public function claim(string $virtualHost, string $queue, string $connectionId): void
    {
        $this->owners[$this->key($virtualHost, $queue)] = $connectionId;
    }

    public function owner(string $virtualHost, string $queue): ?string
    {
        return $this->owners[$this->key($virtualHost, $queue)] ?? null;
    }

    public function isOwner(string $virtualHost, string $queue, ?string $connectionId): bool
    {
        $owner = $this->owner($virtualHost, $queue);

        return $owner !== null && $owner === $connectionId;
    }

    public function release(string $virtualHost, string $queue): void
    {
        unset($this->owners[$this->key($virtualHost, $queue)]);
    }

    /**
     * @return list<array{virtual_host: string, queue: string}>
     */
    public function queuesOwnedBy(string $connectionId): array
    {
        $queues = [];

        foreach ($this->owners as $key => $owner) {
            if ($owner !== $connectionId) {
                continue;
            }

            [$virtualHost, $queue] = explode("\0", $key, 2);
            $queues[] = [
                'virtual_host' => $virtualHost,
                'queue' => $queue,
            ];
        }

        return $queues;
    }

    public function releaseByConnection(string $connectionId): void
    {
        foreach ($this->owners as $key => $owner) {
            if ($owner === $connectionId) {
                unset($this->owners[$key]);
            }
        }
    }

    public function clear(): void
    {
        $this->owners = [];
    }

    private function key(string $virtualHost, string $queue): string
    {
        return $virtualHost . "\0" . $queue;
    }
}
