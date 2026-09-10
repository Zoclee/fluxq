<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\Message;
use FluxQ\Support\Uuid;
use InvalidArgumentException;
use JsonException;
use PDO;
use RuntimeException;

final readonly class MessageRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    /**
     * @param array<string, mixed> $headers
     * @param array<string, mixed> $metadata
     */
    public function create(
        string $payload,
        array $headers = [],
        ?string $contentType = null,
        ?string $contentEncoding = null,
        int $priority = 0,
        bool $persistent = true,
        ?string $messageId = null,
        array $metadata = []
    ): Message {
        $messageId ??= Uuid::v4();
        Uuid::assertValid($messageId, 'Message ID');

        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO messages (message_id, payload, headers, content_type, content_encoding, priority, persistent, metadata)
VALUES (:message_id, :payload, :headers::jsonb, :content_type, :content_encoding, :priority, :persistent, :metadata::jsonb)
RETURNING id, message_id, payload, headers, content_type, content_encoding, priority, persistent, metadata, created_at
SQL);
        $statement->bindValue('message_id', $messageId);
        $statement->bindValue('payload', $payload, PDO::PARAM_LOB);
        $statement->bindValue('headers', $this->encodeHeaders($headers));
        $statement->bindValue('content_type', $contentType);
        $statement->bindValue('content_encoding', $contentEncoding);
        $statement->bindValue('priority', $priority, PDO::PARAM_INT);
        $statement->bindValue('persistent', $persistent, PDO::PARAM_BOOL);
        $statement->bindValue('metadata', $this->encodeObject($metadata, 'Message metadata'));
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created message.');
        }

        return $this->mapRow($row);
    }

    public function findById(int $id): ?Message
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, message_id, payload, headers, content_type, content_encoding, priority, persistent, metadata, created_at
FROM messages
WHERE id = :id
SQL);
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    public function countAll(): int
    {
        $statement = $this->connection->pdo()->query('SELECT COUNT(*) FROM messages');

        if ($statement === false) {
            throw new RuntimeException('Could not count messages in PostgreSQL.');
        }

        return (int) $statement->fetchColumn();
    }

    public function findByMessageId(string $messageId): ?Message
    {
        Uuid::assertValid($messageId, 'Message ID');

        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, message_id, payload, headers, content_type, content_encoding, priority, persistent, metadata, created_at
FROM messages
WHERE message_id = :message_id
SQL);
        $statement->bindValue('message_id', $messageId);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    /**
     * @param list<int> $ids
     * @return list<Message>
     */
    public function findByIds(array $ids): array
    {
        if ($ids === []) {
            return [];
        }

        $placeholders = [];
        $parameters = [];

        foreach (array_values(array_unique($ids)) as $index => $id) {
            $name = 'id_' . $index;
            $placeholders[] = ':' . $name;
            $parameters[$name] = $id;
        }

        $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
SELECT id, message_id, payload, headers, content_type, content_encoding, priority, persistent, metadata, created_at
FROM messages
WHERE id IN (%s)
ORDER BY id
SQL, implode(', ', $placeholders)));

        foreach ($parameters as $name => $id) {
            $statement->bindValue($name, $id, PDO::PARAM_INT);
        }

        $statement->execute();

        return array_map(
            fn (array $row): Message => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    /**
     * @param array{
     *     id: mixed,
     *     message_id: mixed,
     *     payload: mixed,
     *     headers: mixed,
     *     content_type: mixed,
     *     content_encoding: mixed,
     *     priority: mixed,
     *     persistent: mixed,
     *     metadata: mixed,
     *     created_at: mixed
     * } $row
     */
    private function mapRow(array $row): Message
    {
        return new Message(
            (int) $row['id'],
            (string) $row['message_id'],
            $this->normalizePayload($row['payload']),
            $this->decodeHeaders((string) $row['headers']),
            $row['content_type'] === null ? null : (string) $row['content_type'],
            $row['content_encoding'] === null ? null : (string) $row['content_encoding'],
            (int) $row['priority'],
            $this->toBool($row['persistent']),
            $this->decodeObject((string) $row['metadata'], 'Message metadata'),
            new DateTimeImmutable((string) $row['created_at'])
        );
    }

    /**
     * @param array<string, mixed> $headers
     *
     * @throws JsonException
     */
    private function encodeHeaders(array $headers): string
    {
        return $this->encodeObject($headers, 'Message headers');
    }

    /**
     * @return array<string, mixed>
     *
     * @throws JsonException
     */
    private function decodeHeaders(string $headers): array
    {
        return $this->decodeObject($headers, 'Message headers');
    }

    /**
     * @param array<string, mixed> $value
     *
     * @throws JsonException
     */
    private function encodeObject(array $value, string $label): string
    {
        if ($value !== [] && array_is_list($value)) {
            throw new InvalidArgumentException($label . ' must be a JSON object.');
        }

        return json_encode((object) $value, JSON_THROW_ON_ERROR);
    }

    /**
     * @return array<string, mixed>
     *
     * @throws JsonException
     */
    private function decodeObject(string $value, string $label): array
    {
        $decoded = json_decode($value, true, flags: JSON_THROW_ON_ERROR);

        if (!is_array($decoded) || ($decoded !== [] && array_is_list($decoded))) {
            throw new RuntimeException($label . ' stored in PostgreSQL is not a JSON object.');
        }

        return $decoded;
    }

    private function normalizePayload(mixed $payload): string
    {
        if (is_resource($payload)) {
            $contents = stream_get_contents($payload);

            if ($contents === false) {
                throw new RuntimeException('Could not read message payload stream returned by PostgreSQL.');
            }

            return $contents;
        }

        return (string) $payload;
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
