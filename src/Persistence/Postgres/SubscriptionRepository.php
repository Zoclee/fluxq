<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\Subscription;
use InvalidArgumentException;
use JsonException;
use PDO;
use RuntimeException;

final readonly class SubscriptionRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    /**
     * @param array<string, mixed> $metadata
     */
    public function create(
        int $destinationId,
        string $name,
        bool $durable = true,
        array $metadata = []
    ): Subscription {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO subscriptions (destination_id, name, durable, metadata)
VALUES (:destination_id, :name, :durable, :metadata::jsonb)
RETURNING id, destination_id, name, durable, metadata, created_at, updated_at
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->bindValue('name', $name);
        $statement->bindValue('durable', $durable, PDO::PARAM_BOOL);
        $statement->bindValue('metadata', $this->encodeMetadata($metadata));
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created subscription.');
        }

        return $this->mapRow($row);
    }

    public function findById(int $id): ?Subscription
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, destination_id, name, durable, metadata, created_at, updated_at
FROM subscriptions
WHERE id = :id
SQL);
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    public function findByName(int $destinationId, string $name): ?Subscription
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, destination_id, name, durable, metadata, created_at, updated_at
FROM subscriptions
WHERE destination_id = :destination_id
  AND name = :name
SQL);
        $statement->execute([
            'destination_id' => $destinationId,
            'name' => $name,
        ]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    /**
     * @return list<Subscription>
     */
    public function allByDestination(int $destinationId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, destination_id, name, durable, metadata, created_at, updated_at
FROM subscriptions
WHERE destination_id = :destination_id
ORDER BY name
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return array_map(
            fn (array $row): Subscription => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function countByDestination(int $destinationId): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM subscriptions
WHERE destination_id = :destination_id
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    /**
     * @param list<int> $destinationIds
     * @return list<Subscription>
     */
    public function allByDestinations(array $destinationIds): array
    {
        if ($destinationIds === []) {
            return [];
        }

        $placeholders = [];
        $parameters = [];

        foreach (array_values($destinationIds) as $index => $destinationId) {
            $name = 'destination_id_' . $index;
            $placeholders[] = ':' . $name;
            $parameters[$name] = $destinationId;
        }

        $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
SELECT id, destination_id, name, durable, metadata, created_at, updated_at
FROM subscriptions
WHERE destination_id IN (%s)
ORDER BY destination_id, name
SQL, implode(', ', $placeholders)));

        foreach ($parameters as $name => $destinationId) {
            $statement->bindValue($name, $destinationId, PDO::PARAM_INT);
        }

        $statement->execute();

        return array_map(
            fn (array $row): Subscription => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function delete(int $id): bool
    {
        $statement = $this->connection->pdo()->prepare('DELETE FROM subscriptions WHERE id = :id');
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        return $statement->rowCount() === 1;
    }

    /**
     * @param array{
     *     id: mixed,
     *     destination_id: mixed,
     *     name: mixed,
     *     durable: mixed,
     *     metadata: mixed,
     *     created_at: mixed,
     *     updated_at: mixed
     * } $row
     */
    private function mapRow(array $row): Subscription
    {
        return new Subscription(
            (int) $row['id'],
            (int) $row['destination_id'],
            (string) $row['name'],
            $this->toBool($row['durable']),
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
            throw new InvalidArgumentException('Subscription metadata must be a JSON object.');
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
            throw new RuntimeException('Subscription metadata stored in PostgreSQL is not a JSON object.');
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
