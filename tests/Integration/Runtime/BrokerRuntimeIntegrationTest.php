<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Runtime;

use FluxQ\Broker\Broker;
use FluxQ\Console\Application;
use FluxQ\Console\Commands\ServerStartCommand;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\PublishTransaction;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use FluxQ\Runtime\BrokerRuntime;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Runtime\ConsumerRegistry;
use FluxQ\Runtime\RuntimeState;
use FluxQ\Tests\Fixtures\TlsCertificate;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class BrokerRuntimeIntegrationTest extends TestCase
{
    private Connection $connection;
    private ConnectionConfig $config;
    private PDO $pdo;

    #[Before]
    public function setUpRuntimeDatabase(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');

        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL runtime integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->config = $this->connection->config();
        $this->pdo = $this->connection->pdo();

        $this->assertSafeTestDatabase();
        $this->resetSchema();
        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();
    }

    public function testRuntimeConstructsAgainstDatabaseAndStopsWithoutMutatingBrokerState(): void
    {
        $before = $this->brokerTableCounts();
        $connections = new ConnectionRegistry();
        $consumers = new ConsumerRegistry();
        $runtime = new BrokerRuntime(
            new Broker(
                new VirtualHostRepository($this->connection),
                new PublishTransaction($this->connection),
                new DestinationRepository($this->connection),
                new SubscriptionRepository($this->connection),
                new DeliveryRepository($this->connection)
            ),
            $connections,
            $consumers,
            0,
            static function (): void {
            }
        );

        self::assertSame(RuntimeState::Created, $runtime->state());
        self::assertSame(0, $connections->count());
        self::assertSame(0, $consumers->count());

        $runtime->run(maxIterations: 1);

        self::assertSame(RuntimeState::Stopped, $runtime->state());
        self::assertSame(1, $runtime->tickCount());
        self::assertSame(0, $connections->count());
        self::assertSame(0, $consumers->count());
        self::assertSame($before, $this->brokerTableCounts());
    }

    public function testServerStartCommandBootstrapsRuntimeWithoutHanging(): void
    {
        $stream = fopen('php://memory', 'w+');

        self::assertIsResource($stream);

        $command = new ServerStartCommand(
            $this->config,
            Application::VERSION,
            function (Broker $broker, ConnectionRegistry $connections, ConsumerRegistry $consumers): BrokerRuntime {
                $runtime = null;
                $runtime = new BrokerRuntime(
                    $broker,
                    $connections,
                    $consumers,
                    0,
                    static function () use (&$runtime): void {
                        $runtime?->requestShutdown();
                    }
                );

                return $runtime;
            }
        );

        $before = $this->brokerTableCounts();
        $exitCode = $command->run($stream);
        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);
        self::assertSame(0, $exitCode);
        self::assertStringContainsString('FluxQ Message Broker', $output);
        self::assertStringContainsString('Status:   starting', $output);
        self::assertStringContainsString('Database: connected', $output);
        self::assertStringContainsString('AMQP 0-9-1      127.0.0.1:5672', $output);
        self::assertStringNotContainsString('AMQP 0-9-1 TLS', $output);
        self::assertStringContainsString('Runtime started.', $output);
        self::assertStringContainsString('Runtime stopped.', $output);
        self::assertSame($before, $this->brokerTableCounts());
    }

    public function testServerStartCommandReportsTlsListenerWhenEnabled(): void
    {
        try {
            $tls = TlsCertificate::create();
        } catch (\RuntimeException $exception) {
            self::markTestSkipped($exception->getMessage());
        }

        $stream = fopen('php://memory', 'w+');
        self::assertIsResource($stream);

        $command = new ServerStartCommand(
            $this->config,
            Application::VERSION,
            '127.0.0.1',
            5672,
            60,
            '127.0.0.1',
            5673,
            function (Broker $broker, ConnectionRegistry $connections, ConsumerRegistry $consumers): BrokerRuntime {
                $runtime = null;
                $runtime = new BrokerRuntime(
                    $broker,
                    $connections,
                    $consumers,
                    0,
                    static function () use (&$runtime): void {
                        $runtime?->requestShutdown();
                    }
                );

                return $runtime;
            },
            true,
            true,
            '127.0.0.1',
            5671,
            $tls['cert'],
            $tls['key']
        );

        $exitCode = $command->run($stream);
        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);
        self::assertSame(0, $exitCode);
        self::assertStringContainsString('AMQP 0-9-1      127.0.0.1:5672', $output);
        self::assertStringContainsString('AMQP 0-9-1 TLS  127.0.0.1:5671', $output);
    }

    public function testServerStartCommandFailsForInvalidTlsCertificate(): void
    {
        $stream = fopen('php://memory', 'w+');
        self::assertIsResource($stream);

        $command = new ServerStartCommand(
            $this->config,
            Application::VERSION,
            '127.0.0.1',
            5672,
            60,
            '127.0.0.1',
            5673,
            null,
            true,
            true,
            '127.0.0.1',
            5671,
            __DIR__ . '/missing.crt',
            __FILE__
        );

        $exitCode = $command->run($stream);
        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);
        self::assertSame(1, $exitCode);
        self::assertStringContainsString('Startup:  failed', $output);
        self::assertStringContainsString('AMQP TLS certificate file', $output);
        self::assertStringNotContainsString('Runtime started.', $output);
    }

    /**
     * @return array<string, int>
     */
    private function brokerTableCounts(): array
    {
        return [
            'virtual_hosts' => $this->tableCount('virtual_hosts'),
            'destinations' => $this->tableCount('destinations'),
            'bindings' => $this->tableCount('bindings'),
            'subscriptions' => $this->tableCount('subscriptions'),
            'messages' => $this->tableCount('messages'),
            'message_routes' => $this->tableCount('message_routes'),
            'deliveries' => $this->tableCount('deliveries'),
        ];
    }

    private function tableCount(string $table): int
    {
        return (int) $this->pdo->query(sprintf('SELECT count(*) FROM %s', $table))->fetchColumn();
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
