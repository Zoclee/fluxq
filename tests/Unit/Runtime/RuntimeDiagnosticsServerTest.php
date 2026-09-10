<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Runtime;

use DateTimeImmutable;
use FluxQ\Broker\ResourceLimits;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Runtime\ConsumerRegistry;
use FluxQ\Runtime\RuntimeConnection;
use FluxQ\Runtime\RuntimeConsumer;
use FluxQ\Runtime\RuntimeDiagnosticsServer;
use FluxQ\Runtime\RuntimeState;
use PHPUnit\Framework\TestCase;

final class RuntimeDiagnosticsServerTest extends TestCase
{
    public function testConnectionListReflectsActiveRuntimeConnectionsAndRemovals(): void
    {
        $connections = new ConnectionRegistry();
        $consumers = new ConsumerRegistry();
        $connection = new RuntimeConnection(
            '00000000-0000-4000-8000-000000000301',
            'amqp-0-9-1',
            new DateTimeImmutable('2026-08-22T10:00:00+00:00'),
            '127.0.0.1:50000'
        );
        $connections->add($connection);
        $server = new RuntimeDiagnosticsServer($connections, $consumers, port: 0);
        $server->start();

        try {
            $response = $this->request($server, ['command' => 'connections']);
            self::assertTrue($response['ok']);
            self::assertSame($connection->id, $response['data'][0]['id']);
            self::assertSame('amqp-0-9-1', $response['data'][0]['protocol']);
            self::assertSame('127.0.0.1:50000', $response['data'][0]['remote_address']);

            $connections->remove($connection->id);
            $response = $this->request($server, ['command' => 'connections']);
            self::assertTrue($response['ok']);
            self::assertSame([], $response['data']);
        } finally {
            $server->stop();
        }
    }

    public function testConsumerListReflectsActiveConsumersAndRemovals(): void
    {
        $connections = new ConnectionRegistry();
        $consumers = new ConsumerRegistry();
        $consumer = new RuntimeConsumer(
            '00000000-0000-4000-8000-000000000302',
            '00000000-0000-4000-8000-000000000301',
            '/',
            'orders',
            'amqp',
            new DateTimeImmutable('2026-08-22T10:01:00+00:00')
        );
        $consumers->add($consumer);
        $server = new RuntimeDiagnosticsServer($connections, $consumers, port: 0);
        $server->start();

        try {
            $response = $this->request($server, ['command' => 'consumers']);
            self::assertTrue($response['ok']);
            self::assertSame($consumer->id, $response['data'][0]['id']);
            self::assertSame('orders', $response['data'][0]['destination']);
            self::assertSame('amqp', $response['data'][0]['subscription']);

            $consumers->remove($consumer->id);
            $response = $this->request($server, ['command' => 'consumers']);
            self::assertTrue($response['ok']);
            self::assertSame([], $response['data']);
        } finally {
            $server->stop();
        }
    }

    public function testStatsAndMalformedRequestsAreReadOnlyAndDoNotCrashRuntime(): void
    {
        $connections = new ConnectionRegistry();
        $consumers = new ConsumerRegistry();
        $connections->add(new RuntimeConnection(
            '00000000-0000-4000-8000-000000000303',
            'test',
            new DateTimeImmutable()
        ));
        $server = new RuntimeDiagnosticsServer(
            $connections,
            $consumers,
            port: 0,
            limits: new ResourceLimits(maxConnections: 7, maxQueueDepth: 11)
        );
        $server->start();

        try {
            $malformed = $this->rawRequest($server, "not-json\n");
            self::assertFalse($malformed['ok']);

            $unknown = $this->request($server, ['command' => 'shutdown']);
            self::assertFalse($unknown['ok']);
            self::assertSame(1, $connections->count());

            $stats = $this->request($server, ['command' => 'stats']);
            self::assertTrue($stats['ok']);
            self::assertSame(1, $stats['data']['connections']);
            self::assertSame(0, $stats['data']['consumers']);
            self::assertSame('unknown', $stats['data']['state']);
            self::assertSame(0, $stats['data']['unacked']);
            self::assertSame([], $stats['data']['listeners']);
            self::assertSame(7, $stats['data']['limits']['max_connections']);
            self::assertSame(11, $stats['data']['limits']['max_queue_depth']);
        } finally {
            $server->stop();
        }
    }

    public function testStatsCanReportDrainingStateAndUnackedCount(): void
    {
        $server = new RuntimeDiagnosticsServer(
            new ConnectionRegistry(),
            new ConsumerRegistry(),
            port: 0,
            stateProvider: static fn (): RuntimeState => RuntimeState::Draining,
            unackedProvider: static fn (): int => 2,
            listenersProvider: static fn (): array => [
                'amqp' => ['enabled' => true, 'running' => false],
            ]
        );
        $server->start();

        try {
            $stats = $this->request($server, ['command' => 'stats']);

            self::assertTrue($stats['ok']);
            self::assertSame('draining', $stats['data']['state']);
            self::assertSame(2, $stats['data']['unacked']);
            self::assertSame(['enabled' => true, 'running' => false], $stats['data']['listeners']['amqp']);
        } finally {
            $server->stop();
        }
    }

    public function testStopClosesDiagnosticsSocket(): void
    {
        $server = new RuntimeDiagnosticsServer(new ConnectionRegistry(), new ConsumerRegistry(), port: 0);
        $server->start();
        $port = $server->port();
        $server->stop();

        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $port), $errorCode, $errorMessage, 0.1);
        if (is_resource($client)) {
            fclose($client);
        }

        self::assertFalse($client);
    }

    /**
     * @param array<string, mixed> $request
     * @return array<string, mixed>
     */
    private function request(RuntimeDiagnosticsServer $server, array $request): array
    {
        return $this->rawRequest($server, json_encode($request, JSON_THROW_ON_ERROR) . "\n");
    }

    /**
     * @return array<string, mixed>
     */
    private function rawRequest(RuntimeDiagnosticsServer $server, string $payload): array
    {
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $server->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to diagnostics server: %s', $errorMessage));
        stream_set_blocking($client, false);
        fwrite($client, $payload);

        $response = '';
        for ($attempt = 0; $attempt < 100; $attempt++) {
            $server->tick();
            $bytes = fread($client, 8192);
            if (is_string($bytes) && $bytes !== '') {
                $response .= $bytes;
                if (str_contains($response, "\n")) {
                    break;
                }
            }
            usleep(1000);
        }

        fclose($client);
        self::assertNotSame('', $response);
        $decoded = json_decode(trim($response), true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($decoded);

        return $decoded;
    }
}
