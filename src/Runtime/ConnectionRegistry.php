<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

final class ConnectionRegistry
{
    /**
     * @var array<string, RuntimeConnection>
     */
    private array $connections = [];

    public function add(RuntimeConnection $connection): void
    {
        if (isset($this->connections[$connection->id])) {
            throw new RuntimeRegistrationException(sprintf(
                'Runtime connection "%s" is already registered.',
                $connection->id
            ));
        }

        $this->connections[$connection->id] = $connection;
    }

    public function remove(string $connectionId): bool
    {
        if (!isset($this->connections[$connectionId])) {
            return false;
        }

        unset($this->connections[$connectionId]);

        return true;
    }

    public function find(string $connectionId): ?RuntimeConnection
    {
        return $this->connections[$connectionId] ?? null;
    }

    /**
     * @return list<RuntimeConnection>
     */
    public function all(): array
    {
        return array_values($this->connections);
    }

    public function count(): int
    {
        return count($this->connections);
    }

    public function clear(): void
    {
        $this->connections = [];
    }
}
