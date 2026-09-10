<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\RoutingSource;
use FluxQ\Broker\RoutingSourceType;
use InvalidArgumentException;
use JsonException;
use PDO;
use RuntimeException;

final readonly class RoutingSourceRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    public function findByName(int $virtualHostId, string $name): ?RoutingSource
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
FROM routing_sources
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

    /**
     * @param array<string, mixed> $metadata
     */
    public function create(
        int $virtualHostId,
        string $name,
        string|RoutingSourceType $type,
        bool $durable = false,
        bool $autoDelete = false,
        array $metadata = []
    ): RoutingSource {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO routing_sources (virtual_host_id, name, type, durable, auto_delete, metadata)
VALUES (:virtual_host_id, :name, :type, :durable, :auto_delete, :metadata::jsonb)
RETURNING id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('name', $name);
        $statement->bindValue('type', $type instanceof RoutingSourceType ? $type->value : $type);
        $statement->bindValue('durable', $durable, PDO::PARAM_BOOL);
        $statement->bindValue('auto_delete', $autoDelete, PDO::PARAM_BOOL);
        $statement->bindValue('metadata', $this->encodeMetadata($metadata));
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created routing source.');
        }

        return $this->mapRow($row);
    }

    /**
     * @return list<RoutingSource>
     */
    public function allByVirtualHost(int $virtualHostId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, name, type, durable, auto_delete, metadata, created_at, updated_at
FROM routing_sources
WHERE virtual_host_id = :virtual_host_id
ORDER BY name
SQL);
        $statement->execute(['virtual_host_id' => $virtualHostId]);

        return array_map(
            fn (array $row): RoutingSource => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function delete(int $virtualHostId, string $name): bool
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
DELETE FROM routing_sources
WHERE virtual_host_id = :virtual_host_id
  AND name = :name
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('name', $name);
        $statement->execute();

        return $statement->rowCount() === 1;
    }

    public function deleteGraph(int $virtualHostId, string $name): bool
    {
        return $this->connection->transaction(function (PDO $pdo) use ($virtualHostId, $name): bool {
            $bindings = $pdo->prepare(<<<'SQL'
DELETE FROM bindings
WHERE virtual_host_id = :virtual_host_id
  AND source = :source
SQL);
            $bindings->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
            $bindings->bindValue('source', $name);
            $bindings->execute();

            $source = $pdo->prepare(<<<'SQL'
DELETE FROM routing_sources
WHERE virtual_host_id = :virtual_host_id
  AND name = :name
SQL);
            $source->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
            $source->bindValue('name', $name);
            $source->execute();

            return $source->rowCount() === 1;
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
    private function mapRow(array $row): RoutingSource
    {
        return new RoutingSource(
            (int) $row['id'],
            (int) $row['virtual_host_id'],
            (string) $row['name'],
            RoutingSourceType::from((string) $row['type']),
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
            throw new InvalidArgumentException('Routing source metadata must be a JSON object.');
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
            throw new RuntimeException('Routing source metadata stored in PostgreSQL is not a JSON object.');
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
