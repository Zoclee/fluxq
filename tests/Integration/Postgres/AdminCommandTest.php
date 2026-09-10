<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use DateTimeImmutable;
use DateTimeZone;
use FluxQ\Console\Commands\BindingListCommand;
use FluxQ\Console\Commands\BrokerStatsCommand;
use FluxQ\Console\Commands\MessagePeekCommand;
use FluxQ\Console\Commands\QueueListCommand;
use FluxQ\Console\Commands\QueueShowCommand;
use FluxQ\Console\Commands\ReadOnlyDatabaseContext;
use FluxQ\Console\Commands\SubscriptionListCommand;
use FluxQ\Console\Commands\VhostCreateCommand;
use FluxQ\Console\Commands\VhostListCommand;
use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use FluxQ\Runtime\RuntimeDiagnostics;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class AdminCommandTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private ReadOnlyDatabaseContext $context;
    private VirtualHostRepository $virtualHosts;
    private DestinationRepository $destinations;
    private BindingRepository $bindings;
    private MessageRepository $messages;
    private MessageRouteRepository $routes;
    private SubscriptionRepository $subscriptions;
    private int $defaultVirtualHostId;

    #[Before]
    public function setUpDatabase(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');

        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL command integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();
        $this->assertSafeTestDatabase();
        $this->resetSchema();

        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();

        $this->context = new ReadOnlyDatabaseContext($this->connection->config());
        $this->virtualHosts = new VirtualHostRepository($this->connection);
        $this->destinations = new DestinationRepository($this->connection);
        $this->bindings = new BindingRepository($this->connection);
        $this->messages = new MessageRepository($this->connection);
        $this->routes = new MessageRouteRepository($this->connection);
        $this->subscriptions = new SubscriptionRepository($this->connection);
        $this->defaultVirtualHostId = $this->virtualHosts->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
    }

    public function testVhostListListsDefaultAndAdditionalVirtualHosts(): void
    {
        $this->virtualHosts->create('development');

        [$exitCode, $output] = $this->runSimpleCommand(new VhostListCommand($this->context));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Virtual Hosts', $output);
        self::assertStringContainsString('/', $output);
        self::assertStringContainsString('development', $output);
        self::assertStringContainsString('2 virtual hosts.', $output);
    }

    public function testVhostCreatePersistsVirtualHostAndListShowsIt(): void
    {
        [$exitCode, $output] = $this->runArgumentCommand(new VhostCreateCommand($this->connection), ['/development']);

        self::assertSame(0, $exitCode);
        self::assertSame("Virtual host created: /development\n", $output);
        self::assertNotNull($this->virtualHosts->findByName('/development'));
        self::assertNotNull($this->virtualHosts->findByName('/'));

        [$listExitCode, $listOutput] = $this->runSimpleCommand(new VhostListCommand($this->context));

        self::assertSame(0, $listExitCode);
        self::assertStringContainsString('/', $listOutput);
        self::assertStringContainsString('/development', $listOutput);
        self::assertStringContainsString('2 virtual hosts.', $listOutput);
    }

    public function testVhostCreateFailsClearlyForDuplicateVirtualHost(): void
    {
        $this->virtualHosts->create('/development');

        [$exitCode, $output] = $this->runArgumentCommand(new VhostCreateCommand($this->connection), ['/development']);

        self::assertSame(1, $exitCode);
        self::assertSame("ERROR: Virtual host \"/development\" already exists.\n", $output);
    }

    public function testVhostCreateRejectsMissingEmptyAndExtraArguments(): void
    {
        $command = new VhostCreateCommand($this->connection);

        foreach ([[], [''], ['/development', 'extra']] as $arguments) {
            [$exitCode, $output] = $this->runArgumentCommand($command, $arguments);

            self::assertSame(1, $exitCode);
            self::assertSame("Usage: fluxq vhost:create <name>\n", $output);
        }

        self::assertNotNull($this->virtualHosts->findByName('/'));
        self::assertSame(1, $this->virtualHosts->countAll());
    }

    public function testQueueListListsOnlyDefaultVirtualHostQueuesAndHandlesEmptyResult(): void
    {
        [$emptyExitCode, $emptyOutput] = $this->runSimpleCommand(new QueueListCommand($this->context));

        self::assertSame(0, $emptyExitCode);
        self::assertSame("No queues found.\n", $emptyOutput);

        $otherVirtualHost = $this->virtualHosts->create('other');
        $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue', durable: true);
        $this->destinations->create($otherVirtualHost->id, 'outside', 'queue', durable: true);

        [$exitCode, $output] = $this->runSimpleCommand(new QueueListCommand($this->context));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Queues', $output);
        self::assertStringContainsString('orders', $output);
        self::assertStringContainsString('yes', $output);
        self::assertStringNotContainsString('outside', $output);
    }

    public function testQueueShowDisplaysExistingQueueAndRejectsUnknownQueue(): void
    {
        $queue = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue', durable: true, metadata: [
            'retry_policy' => [
                'max_attempts' => 3,
                'retry_delay_seconds' => 15,
                'dead_letter_destination' => 'orders.dlq',
            ],
        ]);
        $this->bindings->create($this->defaultVirtualHostId, 'orders', $queue->id, 'order.created');
        $this->subscriptions->create($queue->id, 'workers');
        $message = $this->messages->create('payload');
        $route = $this->routes->create($message->id, $queue->id);
        $delivery = (new DeliveryRepository($this->connection))->create($route->id, $this->subscriptions->findByName($queue->id, 'workers')?->id ?? 0);
        $this->pdo->prepare("UPDATE deliveries SET state = 'reserved' WHERE id = :id")->execute(['id' => $delivery->id]);

        [$exitCode, $output] = $this->runArgumentCommand(new QueueShowCommand($this->context), ['orders']);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Queue: orders', $output);
        self::assertStringContainsString('Virtual Host:   /', $output);
        self::assertStringContainsString('Durable:        yes', $output);
        self::assertStringContainsString('Retry Policy:   max_attempts=3, retry_delay_seconds=15', $output);
        self::assertStringContainsString('Dead Letter:    orders.dlq', $output);
        self::assertStringContainsString('Bindings:       1', $output);
        self::assertStringContainsString('Subscriptions:  1', $output);
        self::assertStringContainsString('Routes:         1', $output);
        self::assertStringContainsString('Pending:        0', $output);
        self::assertStringContainsString('Reserved:       1', $output);
        self::assertStringContainsString('Depth:          1', $output);
        self::assertStringContainsString('Acknowledged:   0', $output);
        self::assertStringContainsString('Rejected:       0', $output);

        [$missingExitCode, $missingOutput] = $this->runArgumentCommand(new QueueShowCommand($this->context), ['missing']);

        self::assertSame(1, $missingExitCode);
        self::assertStringContainsString('ERROR: Queue "missing" was not found in virtual host "/".', $missingOutput);
    }

    public function testBindingListDisplaysPersistedBindingsWithDestinationNames(): void
    {
        $queue = $this->destinations->create($this->defaultVirtualHostId, 'order-workers', 'queue');
        $this->bindings->create($this->defaultVirtualHostId, 'orders', $queue->id, 'order.created');

        [$exitCode, $output] = $this->runSimpleCommand(new BindingListCommand($this->context));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Bindings', $output);
        self::assertStringContainsString('orders', $output);
        self::assertStringContainsString('order.created', $output);
        self::assertStringContainsString('order-workers', $output);
    }

    public function testSubscriptionListDisplaysDefaultVirtualHostSubscriptions(): void
    {
        $queue = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $otherVirtualHost = $this->virtualHosts->create('other');
        $otherQueue = $this->destinations->create($otherVirtualHost->id, 'outside', 'queue');
        $this->subscriptions->create($queue->id, 'durable-workers', durable: true);
        $this->subscriptions->create($queue->id, 'transient-workers', durable: false);
        $this->subscriptions->create($otherQueue->id, 'outside-workers', durable: true);

        [$exitCode, $output] = $this->runSimpleCommand(new SubscriptionListCommand($this->context));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Subscriptions', $output);
        self::assertStringContainsString('orders', $output);
        self::assertStringContainsString('durable-workers', $output);
        self::assertStringContainsString('yes', $output);
        self::assertStringContainsString('transient-workers', $output);
        self::assertStringContainsString('no', $output);
        self::assertStringNotContainsString('outside-workers', $output);
    }

    public function testMessagePeekShowsBoundedRoutesInOrderAndHandlesBinaryPayloadsWithoutMutation(): void
    {
        $queue = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $otherQueue = $this->destinations->create($this->defaultVirtualHostId, 'other', 'queue');
        $subscription = $this->subscriptions->create($queue->id, 'workers');

        $early = $this->messages->create('first payload');
        $binary = $this->messages->create("\x00\xffpayload");
        $outside = $this->messages->create('outside payload');

        $earlyRoute = $this->routes->create($early->id, $queue->id, new DateTimeImmutable('2026-08-21 10:00:00', new DateTimeZone('UTC')));
        $binaryRoute = $this->routes->create($binary->id, $queue->id, new DateTimeImmutable('2026-08-21 10:00:01', new DateTimeZone('UTC')));
        $this->routes->create($outside->id, $otherQueue->id, new DateTimeImmutable('2026-08-21 09:00:00', new DateTimeZone('UTC')));

        (new DeliveryRepository($this->connection))->create($earlyRoute->id, $subscription->id);
        $this->pdo->prepare(<<<'SQL'
UPDATE deliveries
SET state = 'reserved', attempts = 3, reserved_at = CURRENT_TIMESTAMP
WHERE message_route_id = :message_route_id
SQL)->execute(['message_route_id' => $earlyRoute->id]);

        $before = $this->brokerStateSnapshot();

        [$exitCode, $output] = $this->runArgumentCommand(new MessagePeekCommand($this->context), ['orders']);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Queue: orders', $output);
        self::assertStringContainsString((string) $earlyRoute->id, $output);
        self::assertStringContainsString((string) $binaryRoute->id, $output);
        self::assertStringContainsString($early->messageId, $output);
        self::assertStringContainsString($binary->messageId, $output);
        self::assertStringNotContainsString($outside->messageId, $output);
        self::assertLessThan(strpos($output, (string) $binaryRoute->id), strpos($output, (string) $earlyRoute->id));
        self::assertStringContainsString('<binary: 9 bytes>', $output);
        self::assertSame($before, $this->brokerStateSnapshot());
    }

    public function testMessagePeekEmptyQueueSucceedsCleanly(): void
    {
        $this->destinations->create($this->defaultVirtualHostId, 'empty', 'queue');

        [$exitCode, $output] = $this->runArgumentCommand(new MessagePeekCommand($this->context), ['empty']);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Queue: empty', $output);
        self::assertStringContainsString('No messages found.', $output);
    }

    public function testBrokerStatsReportsRuntimeUnavailableAndPersistenceCounts(): void
    {
        $queue = $this->destinations->create($this->defaultVirtualHostId, 'orders', 'queue');
        $subscription = $this->subscriptions->create($queue->id, 'workers');
        $messageA = $this->messages->create('a');
        $messageB = $this->messages->create('b');
        $routeA = $this->routes->create($messageA->id, $queue->id);
        $routeB = $this->routes->create($messageB->id, $queue->id);
        $pending = (new DeliveryRepository($this->connection))->create($routeA->id, $subscription->id);
        $reserved = (new DeliveryRepository($this->connection))->create($routeB->id, $subscription->id);
        $this->pdo->prepare("UPDATE deliveries SET state = 'reserved' WHERE id = :id")->execute(['id' => $reserved->id]);

        [$exitCode, $output] = $this->runSimpleCommand(new BrokerStatsCommand($this->context, new UnavailableRuntimeDiagnostics()));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Broker Statistics', $output);
        self::assertStringContainsString('Runtime: unavailable', $output);
        self::assertStringContainsString('Virtual Hosts: 1', $output);
        self::assertStringContainsString('Queues:        1', $output);
        self::assertStringContainsString('Messages:      2', $output);
        self::assertStringContainsString('Routes:        2', $output);
        self::assertStringContainsString('Pending:       1', $output);
        self::assertStringContainsString('Reserved:      1', $output);
        self::assertStringContainsString('Acknowledged:  0', $output);
        self::assertStringContainsString('Rejected:      0', $output);

        self::assertNotNull((new DeliveryRepository($this->connection))->findById($pending->id));
    }

    public function testBrokerStatsReportsRuntimeCountsWhenAvailable(): void
    {
        [$exitCode, $output] = $this->runSimpleCommand(new BrokerStatsCommand($this->context, new AvailableRuntimeDiagnostics()));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('State:       Draining', $output);
        self::assertStringContainsString('Connections: 3 / 10', $output);
        self::assertStringContainsString('Consumers:   5', $output);
        self::assertStringContainsString('Unacked:     2', $output);
    }

    public function testNewRepositoryReadMethodsAreScopedAndDeterministic(): void
    {
        $queueA = $this->destinations->create($this->defaultVirtualHostId, 'a', 'queue');
        $queueB = $this->destinations->create($this->defaultVirtualHostId, 'b', 'queue');
        $otherVirtualHost = $this->virtualHosts->create('other');
        $otherQueue = $this->destinations->create($otherVirtualHost->id, 'outside', 'queue');

        $this->bindings->create($this->defaultVirtualHostId, 'z-source', $queueA->id, 'z');
        $this->bindings->create($this->defaultVirtualHostId, 'a-source', $queueB->id, 'a');
        $this->bindings->create($otherVirtualHost->id, 'outside', $otherQueue->id, 'outside');

        self::assertSame(
            ['a-source', 'z-source'],
            array_map(static fn ($binding): string => $binding->source, $this->bindings->allByVirtualHost($this->defaultVirtualHostId))
        );

        $this->subscriptions->create($queueB->id, 'b-workers');
        $this->subscriptions->create($queueA->id, 'a-workers');
        $this->subscriptions->create($otherQueue->id, 'outside-workers');

        self::assertSame(
            ['a-workers', 'b-workers'],
            array_map(static fn ($subscription): string => $subscription->name, $this->subscriptions->allByDestinations([$queueA->id, $queueB->id]))
        );

        $late = $this->messages->create('late');
        $early = $this->messages->create('early');
        $this->routes->create($late->id, $queueA->id, new DateTimeImmutable('2026-08-21 10:00:02', new DateTimeZone('UTC')));
        $earlyRoute = $this->routes->create($early->id, $queueA->id, new DateTimeImmutable('2026-08-21 10:00:01', new DateTimeZone('UTC')));

        self::assertSame([$earlyRoute->id], array_map(static fn ($route): int => $route->id, $this->routes->peekByDestination($queueA->id, 1)));
        self::assertSame(2, $this->routes->countByDestination($queueA->id));
        self::assertSame([$late->id, $early->id], array_map(static fn ($message): int => $message->id, $this->messages->findByIds([$late->id, $early->id])));
    }

    /**
     * @return array{0: int, 1: string}
     */
    private function runSimpleCommand(object $command): array
    {
        $stream = fopen('php://memory', 'w+');
        self::assertIsResource($stream);

        $exitCode = $command->run($stream);

        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);

        return [$exitCode, $output];
    }

    /**
     * @param list<string> $arguments
     * @return array{0: int, 1: string}
     */
    private function runArgumentCommand(object $command, array $arguments): array
    {
        $stream = fopen('php://memory', 'w+');
        self::assertIsResource($stream);

        $exitCode = $command->run($arguments, $stream);

        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);

        return [$exitCode, $output];
    }

    /**
     * @return array<string, list<array<string, mixed>>>
     */
    private function brokerStateSnapshot(): array
    {
        return [
            'deliveries' => $this->fetchRows('SELECT id, state, attempts, reserved_at FROM deliveries ORDER BY id'),
            'message_routes' => $this->fetchRows('SELECT id, message_id, destination_id, available_at, expires_at FROM message_routes ORDER BY id'),
            'messages' => $this->fetchRows('SELECT id, message_id, encode(payload, \'hex\') AS payload FROM messages ORDER BY id'),
        ];
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function fetchRows(string $sql): array
    {
        $statement = $this->pdo->query($sql);

        self::assertNotFalse($statement);

        return $statement->fetchAll(PDO::FETCH_ASSOC);
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function assertSafeTestDatabase(): void
    {
        $databaseName = $this->pdo->query('SELECT current_database()')->fetchColumn();

        if (!is_string($databaseName) || !str_contains($databaseName, 'test')) {
            self::fail(sprintf(
                'Refusing to reset PostgreSQL database "%s"; FLUXQ_TEST_DATABASE_URL must point to a test database.',
                is_scalar($databaseName) ? (string) $databaseName : 'unknown'
            ));
        }
    }
}

final readonly class UnavailableRuntimeDiagnostics implements RuntimeDiagnostics
{
    public function stats(): array
    {
        throw new \RuntimeException('unavailable');
    }

    public function connections(): array
    {
        throw new \RuntimeException('unavailable');
    }

    public function consumers(): array
    {
        throw new \RuntimeException('unavailable');
    }
}

final readonly class AvailableRuntimeDiagnostics implements RuntimeDiagnostics
{
    public function stats(): array
    {
        return ['state' => 'draining', 'connections' => 3, 'consumers' => 5, 'unacked' => 2, 'limits' => ['max_connections' => 10]];
    }

    public function connections(): array
    {
        return [];
    }

    public function consumers(): array
    {
        return [];
    }
}
