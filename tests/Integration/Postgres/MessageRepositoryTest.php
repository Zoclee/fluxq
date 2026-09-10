<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\Migrator;
use InvalidArgumentException;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class MessageRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private MessageRepository $messages;

    #[Before]
    public function setUpRepository(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');

        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL repository integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();

        $this->assertSafeTestDatabase();
        $this->resetSchema();
        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();

        $this->messages = new MessageRepository($this->connection);
    }

    public function testMessageCanBeCreatedWithDefaults(): void
    {
        $message = $this->messages->create('hello');

        self::assertGreaterThan(0, $message->id);
        self::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $message->messageId
        );
        self::assertSame('hello', $message->payload);
        self::assertSame([], $message->headers);
        self::assertNull($message->contentType);
        self::assertNull($message->contentEncoding);
        self::assertSame(0, $message->priority);
        self::assertTrue($message->persistent);
        self::assertNotSame('', $message->createdAt->format(DATE_ATOM));
        self::assertSame(0, (int) $this->pdo->query('SELECT count(*) FROM message_routes')->fetchColumn());
    }

    public function testExplicitMessageIdCanBeSuppliedAndDuplicateFails(): void
    {
        $messageId = '00000000-0000-4000-8000-000000000001';

        $message = $this->messages->create('first', messageId: $messageId);

        self::assertSame($messageId, $message->messageId);

        $this->expectException(PDOException::class);

        $this->messages->create('second', messageId: $messageId);
    }

    public function testInvalidMessageIdIsRejectedBeforePostgreSQL(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->messages->create('payload', messageId: 'not-a-uuid');
    }

    public function testFindByIdReturnsMessageAndUnknownIdReturnsNull(): void
    {
        $created = $this->messages->create('lookup', headers: ['kind' => 'id']);
        $found = $this->messages->findById($created->id);

        self::assertNotNull($found);
        self::assertSame($created->id, $found->id);
        self::assertSame($created->messageId, $found->messageId);
        self::assertSame('lookup', $found->payload);
        self::assertSame(['kind' => 'id'], $found->headers);
        self::assertNull($this->messages->findById(999999));
    }

    public function testFindByMessageIdReturnsMessageAndUnknownUuidReturnsNull(): void
    {
        $messageId = '00000000-0000-4000-8000-000000000002';
        $created = $this->messages->create('lookup', messageId: $messageId);
        $found = $this->messages->findByMessageId($messageId);

        self::assertNotNull($found);
        self::assertSame($created->id, $found->id);
        self::assertSame($messageId, $found->messageId);
        self::assertNull($this->messages->findByMessageId('00000000-0000-4000-8000-000000999999'));
    }

    public function testHeadersRoundTripIncludingNestedJsonCompatibleValues(): void
    {
        $headers = [
            'string' => 'value',
            'integer' => 42,
            'float' => 12.5,
            'boolean' => true,
            'null' => null,
            'nested' => [
                'object' => ['key' => 'value'],
                'list' => ['a', 'b'],
            ],
        ];

        $message = $this->messages->create('metadata', headers: $headers);
        $found = $this->messages->findById($message->id);

        self::assertNotNull($found);
        self::assertEquals($headers, $found->headers);
    }

    public function testMessageMetadataRoundTrips(): void
    {
        $metadata = [
            'amqp_basic_properties' => [
                'correlation_id' => 'correlation-0027',
                'reply_to' => 'reply.queue',
                'expiration' => '60000',
                'message_id' => 'message-0027',
                'timestamp' => 1777777777,
                'type' => 'fluxq.test.message',
                'user_id' => 'guest',
                'app_id' => 'fluxq-pika-test',
            ],
        ];

        $message = $this->messages->create('metadata', metadata: $metadata);
        $found = $this->messages->findById($message->id);

        self::assertNotNull($found);
        self::assertEquals($metadata, $found->metadata);
    }

    public function testHeaderListShapeIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->messages->create('payload', headers: ['not', 'an', 'object']);
    }

    public function testContentTypeAndEncodingRoundTrip(): void
    {
        $message = $this->messages->create(
            'payload',
            contentType: 'application/octet-stream',
            contentEncoding: 'identity'
        );

        self::assertSame('application/octet-stream', $message->contentType);
        self::assertSame('identity', $message->contentEncoding);

        $found = $this->messages->findById($message->id);

        self::assertNotNull($found);
        self::assertSame('application/octet-stream', $found->contentType);
        self::assertSame('identity', $found->contentEncoding);
    }

    public function testPriorityRoundTripsAndInvalidPriorityFails(): void
    {
        $message = $this->messages->create('priority', priority: 255);

        self::assertSame(255, $message->priority);

        $this->expectException(PDOException::class);

        $this->messages->create('invalid', priority: 256);
    }

    public function testPersistentFlagRoundTrips(): void
    {
        $persistent = $this->messages->create('persistent', persistent: true);
        $transient = $this->messages->create('transient', persistent: false);

        self::assertTrue($persistent->persistent);
        self::assertFalse($transient->persistent);
        self::assertFalse($this->messages->findById($transient->id)?->persistent);
    }

    public function testPayloadRoundTripsByteForByte(): void
    {
        $payloads = [
            '',
            'normal text bytes',
            "abc\x00def\x00ghi",
            "\x00\x01\x02\x7F\x80\xFE\xFF",
            random_bytes(64),
        ];

        foreach ($payloads as $payload) {
            $message = $this->messages->create($payload);
            $found = $this->messages->findById($message->id);

            self::assertNotNull($found);
            self::assertSame($payload, $message->payload);
            self::assertSame($payload, $found->payload);
        }
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function assertSafeTestDatabase(): void
    {
        $database = (string) $this->pdo->query('SELECT current_database()')->fetchColumn();

        if (!str_contains(strtolower($database), 'test')) {
            self::markTestSkipped(sprintf(
                'Refusing to reset PostgreSQL database "%s"; FLUXQ_TEST_DATABASE_URL must point to a test database.',
                $database
            ));
        }
    }
}
