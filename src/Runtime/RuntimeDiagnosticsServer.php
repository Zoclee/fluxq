<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

use FluxQ\Broker\ResourceLimits;
use JsonException;
use RuntimeException;

final class RuntimeDiagnosticsServer implements RuntimeComponent
{
    private const MAX_REQUEST_BYTES = 1024;
    private const MAX_RESPONSE_BYTES = 65536;

    /**
     * @var resource|null
     */
    private mixed $server = null;

    /**
     * @var array<int, resource>
     */
    private array $clients = [];

    /**
     * @var array<int, string>
     */
    private array $buffers = [];

    public function __construct(
        private readonly ConnectionRegistry $connections,
        private readonly ConsumerRegistry $consumers,
        private string $host = '127.0.0.1',
        private int $port = 5673,
        private readonly ?ResourceLimits $limits = null,
        private readonly mixed $stateProvider = null,
        private readonly mixed $unackedProvider = null,
        private readonly mixed $listenersProvider = null
    ) {
        if ($this->host === '') {
            throw new RuntimeException('Runtime diagnostics host must not be empty.');
        }

        if ($this->port < 0 || $this->port > 65535) {
            throw new RuntimeException('Runtime diagnostics port must be between 0 and 65535.');
        }
    }

    public function start(): void
    {
        if ($this->server !== null) {
            return;
        }

        $server = @stream_socket_server(sprintf('tcp://%s:%d', $this->host, $this->port), $errorCode, $errorMessage);
        if ($server === false) {
            error_log(sprintf('Runtime diagnostics unavailable on %s:%d: %s (%d)', $this->host, $this->port, $errorMessage, $errorCode));

            return;
        }

        stream_set_blocking($server, false);
        $this->server = $server;
        $localName = stream_socket_get_name($server, false);
        if (is_string($localName) && preg_match('/:(\d+)$/', $localName, $matches) === 1) {
            $this->port = (int) $matches[1];
        }
    }

    public function tick(): void
    {
        if ($this->server === null) {
            return;
        }

        while (($client = @stream_socket_accept($this->server, 0)) !== false) {
            stream_set_blocking($client, false);
            $key = (int) $client;
            $this->clients[$key] = $client;
            $this->buffers[$key] = '';
        }

        foreach ($this->clients as $key => $client) {
            $bytes = fread($client, self::MAX_REQUEST_BYTES);
            if ($bytes === false || $bytes === '') {
                if (feof($client)) {
                    $this->closeClient($key);
                }
                continue;
            }

            $this->buffers[$key] .= $bytes;
            if (strlen($this->buffers[$key]) > self::MAX_REQUEST_BYTES) {
                $this->writeResponse($client, ['ok' => false, 'error' => 'request too large']);
                $this->closeClient($key);
                continue;
            }

            if (!str_contains($this->buffers[$key], "\n")) {
                continue;
            }

            [$line] = explode("\n", $this->buffers[$key], 2);
            $this->writeResponse($client, $this->handleRequest($line));
            $this->closeClient($key);
        }
    }

    public function stop(): void
    {
        foreach (array_keys($this->clients) as $key) {
            $this->closeClient($key);
        }

        if (is_resource($this->server)) {
            fclose($this->server);
        }

        $this->server = null;
    }

    public function host(): string
    {
        return $this->host;
    }

    public function port(): int
    {
        return $this->port;
    }

    /**
     * @return array<string, mixed>
     */
    private function handleRequest(string $line): array
    {
        try {
            $request = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return ['ok' => false, 'error' => 'malformed request'];
        }

        if (!is_array($request) || array_is_list($request) || !isset($request['command']) || !is_string($request['command'])) {
            return ['ok' => false, 'error' => 'malformed request'];
        }

        return match ($request['command']) {
            'stats' => ['ok' => true, 'data' => [
                'state' => $this->runtimeState(),
                'connections' => $this->connections->count(),
                'consumers' => $this->consumers->count(),
                'unacked' => $this->unackedCount(),
                'listeners' => $this->listenersData(),
                'limits' => $this->limitsData(),
            ]],
            'connections' => ['ok' => true, 'data' => array_map(
                static fn (RuntimeConnection $connection): array => [
                    'id' => $connection->id,
                    'protocol' => $connection->protocol,
                    'remote_address' => $connection->remoteAddress,
                    'connected_at' => $connection->connectedAt->format(DATE_ATOM),
                ],
                $this->connections->all()
            )],
            'consumers' => ['ok' => true, 'data' => array_map(
                static fn (RuntimeConsumer $consumer): array => [
                    'id' => $consumer->id,
                    'connection_id' => $consumer->connectionId,
                    'virtual_host' => $consumer->virtualHost,
                    'destination' => $consumer->destination,
                    'subscription' => $consumer->subscription,
                    'created_at' => $consumer->createdAt->format(DATE_ATOM),
                ],
                $this->consumers->all()
            )],
            default => ['ok' => false, 'error' => 'unknown command'],
        };
    }

    /**
     * @return array<string, int>
     */
    private function limitsData(): array
    {
        $limits = $this->limits ?? new ResourceLimits();

        return [
            'max_connections' => $limits->maxConnections,
            'max_channels_per_connection' => $limits->maxChannelsPerConnection,
            'max_consumers_per_connection' => $limits->maxConsumersPerConnection,
            'max_consumers_per_channel' => $limits->maxConsumersPerChannel,
            'amqp_max_frame_size' => $limits->maxFrameSize,
            'max_message_size' => $limits->maxMessageSize,
            'max_queues_per_virtual_host' => $limits->maxQueuesPerVirtualHost,
            'max_queue_depth' => $limits->maxQueueDepth,
        ];
    }

    private function runtimeState(): string
    {
        if (is_callable($this->stateProvider)) {
            $state = ($this->stateProvider)();

            return $state instanceof RuntimeState ? $state->value : (string) $state;
        }

        return 'unknown';
    }

    private function unackedCount(): int
    {
        if (is_callable($this->unackedProvider)) {
            return (int) ($this->unackedProvider)();
        }

        return 0;
    }

    /**
     * @return array<string, mixed>
     */
    private function listenersData(): array
    {
        if (is_callable($this->listenersProvider)) {
            $listeners = ($this->listenersProvider)();

            return is_array($listeners) ? $listeners : [];
        }

        return [];
    }

    /**
     * @param resource $client
     * @param array<string, mixed> $response
     */
    private function writeResponse(mixed $client, array $response): void
    {
        try {
            $payload = json_encode($response, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            $payload = '{"ok":false,"error":"response encoding failed"}';
        }

        if (strlen($payload) > self::MAX_RESPONSE_BYTES) {
            $payload = '{"ok":false,"error":"response too large"}';
        }

        @fwrite($client, $payload . "\n");
    }

    private function closeClient(int $key): void
    {
        if (isset($this->clients[$key]) && is_resource($this->clients[$key])) {
            fclose($this->clients[$key]);
        }

        unset($this->clients[$key], $this->buffers[$key]);
    }
}
