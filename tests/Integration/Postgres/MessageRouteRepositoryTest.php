<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use DateTimeImmutable;
use DateTimeZone;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class MessageRouteRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private MessageRepository $messages;
    private DestinationRepository $destinations;
    private MessageRouteRepository $routes;
    private int $defaultVirtualHostId;

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

        $virtualHosts = new VirtualHostRepository($this->connection);
        $this->messages = new MessageRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $this->routes = new MessageRouteRepository($this->connection);
        $this->defaultVirtualHostId = $virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testRouteCanBeCreatedAndFoundById(): void
    {
        $message = $this->messages->create('hello');
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        $route = $this->routes->create($message->id, $destination->id);

        self::assertGreaterThan(0, $route->id);
        self::assertSame($message->id, $route->messageId);
        self::assertSame($destination->id, $route->destinationId);
        self::assertNotSame('', $route->availableAt->format(DATE_ATOM));
        self::assertNull($route->expiresAt);
        self::assertNotSame('', $route->createdAt->format(DATE_ATOM));

        $found = $this->routes->findById($route->id);

        self::assertNotNull($found);
        self::assertSame($route->id, $found->id);
        self::assertSame($message->id, $found->messageId);
        self::assertSame($destination->id, $found->destinationId);
        self::assertNull($this->routes->findById(999999));
    }

    public function testAllByMessageReturnsOnlyMatchingRoutesInDeterministicOrder(): void
    {
        $message = $this->messages->create('one');
        $otherMessage = $this->messages->create('two');
        $destinationA = $this->destinations->create($this->defaultVirtualHostId, 'a', 'queue');
        $destinationB = $this->destinations->create($this->defaultVirtualHostId, 'b', 'queue');
        $destinationC = $this->destinations->create($this->defaultVirtualHostId, 'c', 'queue');

        $first = $this->routes->create($message->id, $destinationA->id);
        $second = $this->routes->create($message->id, $destinationB->id);
        $this->routes->create($otherMessage->id, $destinationC->id);

        self::assertSame(
            [$first->id, $second->id],
            array_map(
                static fn ($route): int => $route->id,
                $this->routes->allByMessage($message->id)
            )
        );
    }

    public function testAllByDestinationReturnsOnlyMatchingRoutesInAvailabilityOrder(): void
    {
        $messageA = $this->messages->create('a');
        $messageB = $this->messages->create('b');
        $messageC = $this->messages->create('c');
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $otherDestination = $this->destinations->create($this->defaultVirtualHostId, 'billing', 'queue');

        $later = $this->routes->create($messageA->id, $destination->id, new DateTimeImmutable('2030-01-02T00:00:00+00:00'));
        $earlier = $this->routes->create($messageB->id, $destination->id, new DateTimeImmutable('2030-01-01T00:00:00+00:00'));
        $this->routes->create($messageC->id, $otherDestination->id);

        self::assertSame(
            [$earlier->id, $later->id],
            array_map(
                static fn ($route): int => $route->id,
                $this->routes->allByDestination($destination->id)
            )
        );
    }

    public function testOneMessageCanRouteToMultipleDestinationsWithoutDuplicatingPayload(): void
    {
        $message = $this->messages->create('shared-payload');
        $destinationA = $this->destinations->create($this->defaultVirtualHostId, 'a', 'queue');
        $destinationB = $this->destinations->create($this->defaultVirtualHostId, 'b', 'queue');
        $destinationC = $this->destinations->create($this->defaultVirtualHostId, 'c', 'queue');

        $routeA = $this->routes->create($message->id, $destinationA->id);
        $routeB = $this->routes->create($message->id, $destinationB->id);
        $routeC = $this->routes->create($message->id, $destinationC->id);

        self::assertSame(
            [$routeA->id, $routeB->id, $routeC->id],
            array_map(
                static fn ($route): int => $route->id,
                $this->routes->allByMessage($message->id)
            )
        );
        self::assertSame(1, $this->countRows('messages', 'id = ' . $message->id));
        self::assertSame(3, $this->countRows('message_routes', 'message_id = ' . $message->id));
        self::assertSame(0, $this->countRows('deliveries'));
    }

    public function testUnknownMessageIdFails(): void
    {
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        $this->expectException(PDOException::class);

        $this->routes->create(999999, $destination->id);
    }

    public function testUnknownDestinationIdFails(): void
    {
        $message = $this->messages->create('hello');

        $this->expectException(PDOException::class);

        $this->routes->create($message->id, 999999);
    }

    public function testExplicitAvailabilityAndExpirationRoundTrip(): void
    {
        $message = $this->messages->create('hello');
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $availableAt = new DateTimeImmutable('2030-01-01T12:13:14.123456+02:00');
        $expiresAt = new DateTimeImmutable('2030-01-02T12:13:14.654321+02:00');

        $route = $this->routes->create($message->id, $destination->id, $availableAt, $expiresAt);
        $found = $this->routes->findById($route->id);

        self::assertNotNull($found);
        self::assertSame($availableAt->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u'), $found->availableAt->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u'));
        self::assertNotNull($found->expiresAt);
        self::assertSame($expiresAt->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u'), $found->expiresAt->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u'));
    }

    public function testNullExpirationRoundTrips(): void
    {
        $message = $this->messages->create('hello');
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        $route = $this->routes->create($message->id, $destination->id, expiresAt: null);

        self::assertNull($route->expiresAt);
        self::assertNull($this->routes->findById($route->id)?->expiresAt);
    }

    public function testDuplicateRouteFails(): void
    {
        $message = $this->messages->create('hello');
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $this->routes->create($message->id, $destination->id);

        $this->expectException(PDOException::class);

        $this->routes->create($message->id, $destination->id);
    }

    public function testExpirationMustBeAfterCreatedAtByExistingSchemaConstraint(): void
    {
        $message = $this->messages->create('hello');
        $destination = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');

        $this->expectException(PDOException::class);

        $this->routes->create($message->id, $destination->id, expiresAt: new DateTimeImmutable('2000-01-01T00:00:00+00:00'));
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function countRows(string $table, ?string $where = null): int
    {
        $sql = 'SELECT count(*) FROM ' . $table;

        if ($where !== null) {
            $sql .= ' WHERE ' . $where;
        }

        return (int) $this->pdo->query($sql)->fetchColumn();
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
