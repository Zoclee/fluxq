<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\MessageRoute;
use PDO;
use RuntimeException;

final readonly class MessageRouteRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    public function create(
        int $messageId,
        int $destinationId,
        ?DateTimeImmutable $availableAt = null,
        ?DateTimeImmutable $expiresAt = null
    ): MessageRoute {
        if ($availableAt === null) {
            $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO message_routes (message_id, destination_id, expires_at)
VALUES (:message_id, :destination_id, :expires_at)
RETURNING id, message_id, destination_id, available_at, expires_at, created_at
SQL);
        } else {
            $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO message_routes (message_id, destination_id, available_at, expires_at)
VALUES (:message_id, :destination_id, :available_at, :expires_at)
RETURNING id, message_id, destination_id, available_at, expires_at, created_at
SQL);
            $statement->bindValue('available_at', $this->formatTimestamp($availableAt));
        }

        $statement->bindValue('message_id', $messageId, PDO::PARAM_INT);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->bindValue('expires_at', $expiresAt === null ? null : $this->formatTimestamp($expiresAt));
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created message route.');
        }

        return $this->mapRow($row);
    }

    public function findById(int $id): ?MessageRoute
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, message_id, destination_id, available_at, expires_at, created_at
FROM message_routes
WHERE id = :id
SQL);
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    /**
     * @return list<MessageRoute>
     */
    public function allByMessage(int $messageId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, message_id, destination_id, available_at, expires_at, created_at
FROM message_routes
WHERE message_id = :message_id
ORDER BY id
SQL);
        $statement->bindValue('message_id', $messageId, PDO::PARAM_INT);
        $statement->execute();

        return array_map(
            fn (array $row): MessageRoute => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    /**
     * @return list<MessageRoute>
     */
    public function allByDestination(int $destinationId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, message_id, destination_id, available_at, expires_at, created_at
FROM message_routes
WHERE destination_id = :destination_id
ORDER BY available_at, id
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return array_map(
            fn (array $row): MessageRoute => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    /**
     * @return list<MessageRoute>
     */
    public function peekByDestination(int $destinationId, int $limit = 10): array
    {
        if ($limit < 1 || $limit > 100) {
            throw new RuntimeException('Message route peek limit must be between 1 and 100.');
        }

        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, message_id, destination_id, available_at, expires_at, created_at
FROM message_routes
WHERE destination_id = :destination_id
ORDER BY available_at, id
LIMIT :limit
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->bindValue('limit', $limit, PDO::PARAM_INT);
        $statement->execute();

        return array_map(
            fn (array $row): MessageRoute => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function countByDestination(int $destinationId): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM message_routes
WHERE destination_id = :destination_id
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    public function countAll(): int
    {
        $statement = $this->connection->pdo()->query('SELECT COUNT(*) FROM message_routes');

        if ($statement === false) {
            throw new RuntimeException('Could not count message routes in PostgreSQL.');
        }

        return (int) $statement->fetchColumn();
    }

    /**
     * @param array{
     *     id: mixed,
     *     message_id: mixed,
     *     destination_id: mixed,
     *     available_at: mixed,
     *     expires_at: mixed,
     *     created_at: mixed
     * } $row
     */
    private function mapRow(array $row): MessageRoute
    {
        return new MessageRoute(
            (int) $row['id'],
            (int) $row['message_id'],
            (int) $row['destination_id'],
            new DateTimeImmutable((string) $row['available_at']),
            $row['expires_at'] === null ? null : new DateTimeImmutable((string) $row['expires_at']),
            new DateTimeImmutable((string) $row['created_at'])
        );
    }

    private function formatTimestamp(DateTimeImmutable $timestamp): string
    {
        return $timestamp->format('Y-m-d H:i:s.uP');
    }
}
