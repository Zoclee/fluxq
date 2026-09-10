<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\Destination;
use FluxQ\Broker\DestinationType;
use InvalidArgumentException;
use JsonException;
use PDO;
use RuntimeException;

final readonly class DestinationRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    public function findByName(int $virtualHostId, string $name): ?Destination
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
FROM destinations
WHERE virtual_host_id = :virtual_host_id
  AND name = :name
SQL);
        $statement->execute([
            'virtual_host_id' => $virtualHostId,
            'name' => $name,
        ]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    public function findById(int $id): ?Destination
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
FROM destinations
WHERE id = :id
SQL);
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    /**
     * @param array<string, mixed> $metadata
     */
    public function create(
        int $virtualHostId,
        string $name,
        string|DestinationType $type,
        bool $durable = false,
        bool $autoDelete = false,
        array $metadata = []
    ): Destination {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO destinations (virtual_host_id, name, type, durable, auto_delete, metadata)
VALUES (:virtual_host_id, :name, :type, :durable, :auto_delete, :metadata::jsonb)
RETURNING id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('name', $name);
        $statement->bindValue('type', $type instanceof DestinationType ? $type->value : $type);
        $statement->bindValue('durable', $durable, PDO::PARAM_BOOL);
        $statement->bindValue('auto_delete', $autoDelete, PDO::PARAM_BOOL);
        $statement->bindValue('metadata', $this->encodeMetadata($metadata));
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created destination.');
        }

        return $this->mapRow($row);
    }

    /**
     * @return list<Destination>
     */
    public function allByVirtualHost(int $virtualHostId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
FROM destinations
WHERE virtual_host_id = :virtual_host_id
ORDER BY name
SQL);
        $statement->execute(['virtual_host_id' => $virtualHostId]);

        return array_map(
            fn (array $row): Destination => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function countByType(DestinationType $type): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM destinations
WHERE type = :type
SQL);
        $statement->bindValue('type', $type->value);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    public function countQueuesByVirtualHost(int $virtualHostId): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM destinations
WHERE virtual_host_id = :virtual_host_id
  AND type = 'queue'
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    /**
     * @return list<Destination>
     */
    public function allExclusiveQueues(): array
    {
        $statement = $this->connection->pdo()->query(<<<'SQL'
SELECT id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
FROM destinations
WHERE type = 'queue'
  AND metadata @> '{"exclusive": true}'::jsonb
ORDER BY id
SQL);

        if ($statement === false) {
            throw new RuntimeException('Could not read exclusive queues from PostgreSQL.');
        }

        return array_map(
            fn (array $row): Destination => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function deleteQueueGraph(int $destinationId): int
    {
        return $this->connection->transaction(function (PDO $pdo) use ($destinationId): int {
            $count = $pdo->prepare(<<<'SQL'
SELECT COUNT(*)
FROM deliveries
WHERE destination_id = :destination_id
  AND state IN ('pending', 'reserved')
SQL);
            $count->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
            $count->execute();
            $deletedMessageCount = (int) $count->fetchColumn();

            foreach ([
                'DELETE FROM deliveries WHERE destination_id = :destination_id',
                'DELETE FROM subscriptions WHERE destination_id = :destination_id',
                'DELETE FROM bindings WHERE destination_id = :destination_id',
                'DELETE FROM message_routes WHERE destination_id = :destination_id',
                'DELETE FROM destinations WHERE id = :destination_id',
            ] as $sql) {
                $statement = $pdo->prepare($sql);
                $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
                $statement->execute();
            }

            return $deletedMessageCount;
        });
    }

    /**
     * @param array{
     *     id: mixed,
     *     virtual_host_id: mixed,
     *     name: mixed,
     *     type: mixed,
     *     durable: mixed,
     *     auto_delete: mixed,
     *     metadata: mixed,
     *     created_at: mixed,
     *     updated_at: mixed
     * } $row
     */
    private function mapRow(array $row): Destination
    {
        return new Destination(
            (int) $row['id'],
            (int) $row['virtual_host_id'],
            (string) $row['name'],
            DestinationType::from((string) $row['type']),
            $this->toBool($row['durable']),
            $this->toBool($row['auto_delete']),
            $this->decodeMetadata((string) $row['metadata']),
            new DateTimeImmutable((string) $row['created_at']),
            new DateTimeImmutable((string) $row['updated_at'])
        );
    }

    /**
     * @param array<string, mixed> $metadata
     *
     * @throws JsonException
     */
    private function encodeMetadata(array $metadata): string
    {
        if ($metadata !== [] && array_is_list($metadata)) {
            throw new InvalidArgumentException('Destination metadata must be a JSON object.');
        }

        return json_encode((object) $metadata, JSON_THROW_ON_ERROR);
    }

    /**
     * @return array<string, mixed>
     *
     * @throws JsonException
     */
    private function decodeMetadata(string $metadata): array
    {
        $decoded = json_decode($metadata, true, flags: JSON_THROW_ON_ERROR);

        if (!is_array($decoded) || ($decoded !== [] && array_is_list($decoded))) {
            throw new RuntimeException('Destination metadata stored in PostgreSQL is not a JSON object.');
        }

        return $decoded;
    }

    private function toBool(mixed $value): bool
    {
        if (is_bool($value)) {
            return $value;
        }

        return match ((string) $value) {
            't', 'true', '1' => true,
            default => false,
        };
    }
}
