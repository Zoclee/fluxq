<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Console;

use FluxQ\Console\Commands\ConnectionListCommand;
use FluxQ\Console\Commands\ConsumerListCommand;
use FluxQ\Console\Commands\HealthCommand;
use FluxQ\Console\Commands\ReadinessCommand;
use FluxQ\Persistence\Postgres\ConnectionConfig;
use FluxQ\Runtime\RuntimeDiagnostics;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class DiagnosticsCommandTest extends TestCase
{
    public function testConnectionListDisplaysRuntimeConnections(): void
    {
        [$exitCode, $output] = $this->runCommand(new ConnectionListCommand(new FakeRuntimeDiagnostics(
            connections: [[
                'id' => 'connection-1',
                'protocol' => 'amqp-0-9-1',
                'remote_address' => '127.0.0.1:50000',
                'connected_at' => '2026-08-22T10:00:00+00:00',
            ]]
        )));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Connections', $output);
        self::assertStringContainsString('connection-1', $output);
        self::assertStringContainsString('amqp-0-9-1', $output);
    }

    public function testConsumerListDisplaysRuntimeConsumers(): void
    {
        [$exitCode, $output] = $this->runCommand(new ConsumerListCommand(new FakeRuntimeDiagnostics(
            consumers: [[
                'id' => 'consumer-1',
                'connection_id' => 'connection-1',
                'virtual_host' => '/',
                'destination' => 'orders',
                'subscription' => 'amqp',
                'created_at' => '2026-08-22T10:00:00+00:00',
            ]]
        )));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Consumers', $output);
        self::assertStringContainsString('consumer-1', $output);
        self::assertStringContainsString('orders', $output);
    }

    public function testRuntimeUnavailableIsReportedClearly(): void
    {
        [$connectionExitCode, $connectionOutput] = $this->runCommand(new ConnectionListCommand(new FakeRuntimeDiagnostics(unavailable: true)));
        [$consumerExitCode, $consumerOutput] = $this->runCommand(new ConsumerListCommand(new FakeRuntimeDiagnostics(unavailable: true)));

        self::assertSame(1, $connectionExitCode);
        self::assertSame("Runtime: unavailable\n", $connectionOutput);
        self::assertSame(1, $consumerExitCode);
        self::assertSame("Runtime: unavailable\n", $consumerOutput);
    }

    public function testHealthReportsUnavailableRuntime(): void
    {
        [$exitCode, $output] = $this->runCommand(new HealthCommand(new FakeRuntimeDiagnostics(unavailable: true)));

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('FluxQ Health', $output);
        self::assertStringContainsString('Runtime: unavailable', $output);
    }

    public function testHealthReportsHealthyRuntime(): void
    {
        [$exitCode, $output] = $this->runCommand(new HealthCommand(new FakeRuntimeDiagnostics(state: 'running')));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Runtime: healthy', $output);
        self::assertStringContainsString('State:   Running', $output);
    }

    public function testReadinessFailsWhenRuntimeIsUnavailable(): void
    {
        [$exitCode, $output] = $this->runCommand(new ReadinessCommand(
            new FakeRuntimeDiagnostics(unavailable: true),
            new ConnectionConfig('127.0.0.1', 5432, 'unused', 'unused', null),
            __DIR__
        ));

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('Ready: no', $output);
        self::assertStringContainsString('Reason: runtime unavailable', $output);
    }

    public function testReadinessFailsWhenRuntimeIsDraining(): void
    {
        [$exitCode, $output] = $this->runCommand(new ReadinessCommand(
            new FakeRuntimeDiagnostics(state: 'draining'),
            new ConnectionConfig('127.0.0.1', 5432, 'unused', 'unused', null),
            __DIR__
        ));

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('Ready: no', $output);
        self::assertStringContainsString('Reason: runtime is draining', $output);
    }

    /**
     * @return array{0: int, 1: string}
     */
    private function runCommand(object $command): array
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
}

final readonly class FakeRuntimeDiagnostics implements RuntimeDiagnostics
{
    /**
     * @param list<array<string, mixed>> $connections
     * @param list<array<string, mixed>> $consumers
     */
    public function __construct(
        private array $connections = [],
        private array $consumers = [],
        private array $limits = [],
        private bool $unavailable = false,
        private string $state = 'running',
        private array $listeners = [
            'amqp' => ['enabled' => true, 'running' => true],
            'amqp_tls' => ['enabled' => false, 'running' => false],
        ]
    ) {
    }

    public function stats(): array
    {
        if ($this->unavailable) {
            throw new RuntimeException('unavailable');
        }

        return [
            'state' => $this->state,
            'connections' => count($this->connections),
            'consumers' => count($this->consumers),
            'limits' => $this->limits,
            'listeners' => $this->listeners,
        ];
    }

    public function connections(): array
    {
        if ($this->unavailable) {
            throw new RuntimeException('unavailable');
        }

        return $this->connections;
    }

    public function consumers(): array
    {
        if ($this->unavailable) {
            throw new RuntimeException('unavailable');
        }

        return $this->consumers;
    }
}
