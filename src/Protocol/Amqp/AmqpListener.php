<?php

declare(strict_types=1);

namespace FluxQ\Protocol\Amqp;

use FluxQ\Broker\AuthenticationService;
use FluxQ\Broker\AuthorizationService;
use FluxQ\Broker\Broker;
use FluxQ\Broker\ResourceLimits;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Runtime\ConsumerRegistry;
use FluxQ\Runtime\RuntimeComponent;
use FluxQ\Runtime\RuntimeDrainingComponent;
use RuntimeException;

final class AmqpListener implements RuntimeDrainingComponent
{
    /**
     * @var resource|null
     */
    private mixed $server = null;

    /**
     * @var array<string, AmqpConnection>
     */
    private array $connections = [];

    /**
     * @var array<string, resource>
     */
    private array $pendingTlsClients = [];

    private bool $draining = false;

    public function __construct(
        private readonly ConnectionRegistry $runtimeConnections,
        private string $host = '127.0.0.1',
        private int $port = 5672,
        private readonly int $maxFrameSize = 131072,
        private readonly ?Broker $broker = null,
        private readonly ?AuthenticationService $authenticator = null,
        private readonly ?AuthorizationService $authorizer = null,
        private readonly ?ConsumerRegistry $consumers = null,
        private readonly int $maxMessageSize = 10485760,
        private readonly int $heartbeatInterval = 60,
        private readonly mixed $clock = null,
        private readonly ?AmqpTlsConfig $tls = null,
        private readonly ?ResourceLimits $limits = null
    ) {
        if ($this->host === '') {
            throw new RuntimeException('AMQP listener host must not be empty.');
        }

        if ($this->port < 0 || $this->port > 65535) {
            throw new RuntimeException('AMQP listener port must be between 0 and 65535.');
        }

        if ($this->heartbeatInterval < 0 || $this->heartbeatInterval > 65535) {
            throw new RuntimeException('AMQP heartbeat interval must fit in an unsigned short.');
        }
    }

    public function start(): void
    {
        if ($this->server !== null) {
            return;
        }

        $address = sprintf('tcp://%s:%d', $this->host, $this->port);
        $server = @stream_socket_server($address, $errorCode, $errorMessage);

        if ($server === false) {
            throw new RuntimeException(sprintf(
                'Could not start AMQP listener on %s:%d: %s (%d)',
                $this->host,
                $this->port,
                $errorMessage,
                $errorCode
            ));
        }

        stream_set_blocking($server, false);
        $this->server = $server;
        $this->broker?->deletePersistedExclusiveQueues();
        $localName = stream_socket_get_name($server, false);
        if (is_string($localName) && preg_match('/:(\d+)$/', $localName, $matches) === 1) {
            $this->port = (int) $matches[1];
        }
    }

    public function tick(): void
    {
        if ($this->server !== null && !$this->draining) {
            while (($client = @stream_socket_accept($this->server, 0)) !== false) {
                stream_set_blocking($client, false);

                if (!$this->hasConnectionCapacity()) {
                    fclose($client);
                    continue;
                }

                if ($this->tls !== null) {
                    stream_context_set_options($client, ['ssl' => $this->tls->streamContextOptions()]);
                    $this->pendingTlsClients[(string) (int) $client] = $client;
                    continue;
                }

                $this->addConnection($client);
            }

            $this->completePendingTlsHandshakes();
        }

        foreach ($this->connections as $key => $connection) {
            $connection->tick();
            if ($connection->isClosed()) {
                unset($this->connections[$key]);
            }
        }
    }

    public function stop(): void
    {
        foreach ($this->connections as $connection) {
            $connection->close();
        }

        $this->connections = [];

        foreach ($this->pendingTlsClients as $client) {
            if (is_resource($client)) {
                fclose($client);
            }
        }

        $this->pendingTlsClients = [];

        if (is_resource($this->server)) {
            fclose($this->server);
        }

        $this->server = null;
        $this->draining = false;
    }

    public function beginDrain(): void
    {
        if ($this->draining) {
            return;
        }

        $this->draining = true;

        if (is_resource($this->server)) {
            fclose($this->server);
        }

        $this->server = null;

        foreach ($this->pendingTlsClients as $client) {
            if (is_resource($client)) {
                fclose($client);
            }
        }

        $this->pendingTlsClients = [];

        foreach ($this->connections as $connection) {
            $connection->beginDrain();
        }
    }

    public function inFlightCount(): int
    {
        $count = 0;
        foreach ($this->connections as $connection) {
            $count += $connection->unacknowledgedDeliveryCount();
        }

        return $count;
    }

    public function host(): string
    {
        return $this->host;
    }

    public function port(): int
    {
        return $this->port;
    }

    public function connectionCount(): int
    {
        return count($this->connections);
    }

    public function isTls(): bool
    {
        return $this->tls !== null;
    }

    public function isListening(): bool
    {
        return is_resource($this->server);
    }

    private function completePendingTlsHandshakes(): void
    {
        foreach ($this->pendingTlsClients as $key => $client) {
            if (!is_resource($client) || feof($client)) {
                unset($this->pendingTlsClients[$key]);
                if (is_resource($client)) {
                    fclose($client);
                }
                continue;
            }

            $result = @stream_socket_enable_crypto($client, true, AmqpTlsConfig::serverCryptoMethod());
            if ($result === true) {
                unset($this->pendingTlsClients[$key]);
                $this->addConnection($client);
                continue;
            }

            if ($result === false) {
                unset($this->pendingTlsClients[$key]);
                fclose($client);
            }
        }
    }

    /**
     * @param resource $client
     */
    private function addConnection(mixed $client): void
    {
        $key = (string) (int) $client;
        $this->connections[$key] = new AmqpConnection(
            $client,
            $this->runtimeConnections,
            $this->maxFrameSize,
            $this->broker,
            $this->authenticator,
            $this->authorizer,
            $this->consumers,
            $this->maxMessageSize,
            heartbeatInterval: $this->heartbeatInterval,
            limits: $this->limits,
            draining: $this->draining,
            clock: $this->clock,
            queueDeletionNotifier: $this->cancelConsumersForDeletedQueue(...)
        );
    }

    private function cancelConsumersForDeletedQueue(string $virtualHost, string $queue): void
    {
        foreach ($this->connections as $connection) {
            $connection->cancelConsumersForDeletedQueue($virtualHost, $queue);
        }
    }

    private function hasConnectionCapacity(): bool
    {
        $limit = ($this->limits ?? new ResourceLimits())->maxConnections;

        return $limit === 0 || $this->runtimeConnections->count() + count($this->pendingTlsClients) < $limit;
    }
}
