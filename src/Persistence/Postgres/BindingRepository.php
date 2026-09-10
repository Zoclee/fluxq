<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\Binding;
use InvalidArgumentException;
use JsonException;
use PDO;
use RuntimeException;

final readonly class BindingRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    public function findById(int $id): ?Binding
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, source, destination_id, routing_key, metadata, created_at
FROM bindings
WHERE id = :id
SQL);
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    public function findExact(int $virtualHostId, string $source, int $destinationId, string $routingKey): ?Binding
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, source, destination_id, routing_key, metadata, created_at
FROM bindings
WHERE virtual_host_id = :virtual_host_id
  AND source = :source
  AND destination_id = :destination_id
  AND routing_key = :routing_key
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('source', $source);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->bindValue('routing_key', $routingKey);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    /**
     * @param array<string, mixed> $metadata
     */
    public function create(
        int $virtualHostId,
        string $source,
        int $destinationId,
        string $routingKey,
        array $metadata = []
    ): Binding {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO bindings (virtual_host_id, source, destination_id, routing_key, metadata)
VALUES (:virtual_host_id, :source, :destination_id, :routing_key, :metadata::jsonb)
RETURNING id, virtual_host_id, source, destination_id, routing_key, metadata, created_at
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('source', $source);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->bindValue('routing_key', $routingKey);
        $statement->bindValue('metadata', $this->encodeMetadata($metadata));
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created binding.');
        }

        return $this->mapRow($row);
    }

    /**
     * @return list<Binding>
     */
    public function findForRoute(int $virtualHostId, string $source, string $routingKey): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, source, destination_id, routing_key, metadata, created_at
FROM bindings
WHERE virtual_host_id = :virtual_host_id
  AND source = :source
  AND routing_key = :routing_key
ORDER BY id
SQL);
        $statement->execute([
            'virtual_host_id' => $virtualHostId,
            'source' => $source,
            'routing_key' => $routingKey,
        ]);

        return array_map(
            fn (array $row): Binding => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    /**
     * @return list<Binding>
     */
    public function findForSource(int $virtualHostId, string $source): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, source, destination_id, routing_key, metadata, created_at
FROM bindings
WHERE virtual_host_id = :virtual_host_id
  AND source = :source
ORDER BY id
SQL);
        $statement->execute([
            'virtual_host_id' => $virtualHostId,
            'source' => $source,
        ]);

        return array_map(
            fn (array $row): Binding => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    /**
     * @return list<Binding>
     */
    public function allByDestination(int $destinationId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, source, destination_id, routing_key, metadata, created_at
FROM bindings
WHERE destination_id = :destination_id
ORDER BY id
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return array_map(
            fn (array $row): Binding => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function countByDestination(int $destinationId): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM bindings
WHERE destination_id = :destination_id
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    public function countBySource(int $virtualHostId, string $source): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM bindings
WHERE virtual_host_id = :virtual_host_id
  AND source = :source
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('source', $source);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    /**
     * @return list<Binding>
     */
    public function allByVirtualHost(int $virtualHostId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, virtual_host_id, source, destination_id, routing_key, metadata, created_at
FROM bindings
WHERE virtual_host_id = :virtual_host_id
ORDER BY source, routing_key, id
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->execute();

        return array_map(
            fn (array $row): Binding => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function delete(int $id): bool
    {
        $statement = $this->connection->pdo()->prepare('DELETE FROM bindings WHERE id = :id');
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        return $statement->rowCount() === 1;
    }

    public function deleteExact(int $virtualHostId, string $source, int $destinationId, string $routingKey): bool
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
DELETE FROM bindings
WHERE virtual_host_id = :virtual_host_id
  AND source = :source
  AND destination_id = :destination_id
  AND routing_key = :routing_key
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('source', $source);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->bindValue('routing_key', $routingKey);
        $statement->execute();

        return $statement->rowCount() === 1;
    }

    public function deleteBySource(int $virtualHostId, string $source): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
DELETE FROM bindings
WHERE virtual_host_id = :virtual_host_id
  AND source = :source
SQL);
        $statement->bindValue('virtual_host_id', $virtualHostId, PDO::PARAM_INT);
        $statement->bindValue('source', $source);
        $statement->execute();

        return $statement->rowCount();
    }

    /**
     * @param array{
     *     id: mixed,
     *     virtual_host_id: mixed,
     *     source: mixed,
     *     destination_id: mixed,
     *     routing_key: mixed,
     *     metadata: mixed,
     *     created_at: mixed
     * } $row
     */
    private function mapRow(array $row): Binding
    {
        return new Binding(
            (int) $row['id'],
            (int) $row['virtual_host_id'],
            (string) $row['source'],
            (int) $row['destination_id'],
            (string) $row['routing_key'],
            $this->decodeMetadata((string) $row['metadata']),
            new DateTimeImmutable((string) $row['created_at'])
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
            throw new InvalidArgumentException('Binding metadata must be a JSON object.');
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
            throw new RuntimeException('Binding metadata stored in PostgreSQL is not a JSON object.');
        }

        return $decoded;
    }
}
