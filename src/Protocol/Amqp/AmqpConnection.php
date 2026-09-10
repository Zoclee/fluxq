<?php

declare(strict_types=1);

namespace FluxQ\Protocol\Amqp;

use DateTimeImmutable;
use FluxQ\Broker\AcknowledgeRequest;
use FluxQ\Broker\AuthenticatedUser;
use FluxQ\Broker\AuthenticationService;
use FluxQ\Broker\AuthorizationPermission;
use FluxQ\Broker\AuthorizationService;
use FluxQ\Broker\Broker;
use FluxQ\Broker\Delivery;
use FluxQ\Broker\Message;
use FluxQ\Broker\PublishRequest;
use FluxQ\Broker\RejectRequest;
use FluxQ\Broker\ReleaseRequest;
use FluxQ\Broker\ReserveRequest;
use FluxQ\Broker\ResourceLimitException;
use FluxQ\Broker\ResourceLimits;
use FluxQ\Broker\RoutingSourceType;
use FluxQ\Broker\TopologyException;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Runtime\ConsumerRegistry;
use FluxQ\Runtime\RuntimeConnection;
use FluxQ\Runtime\RuntimeConsumer;
use RuntimeException;
use Throwable;

final class AmqpConnection
{
    public const PROTOCOL_HEADER = "AMQP\x00\x00\x09\x01";
    private const NANOS_PER_SECOND = 1_000_000_000;

    private AmqpConnectionState $state = AmqpConnectionState::AwaitingProtocolHeader;
    private string $headerBuffer = '';
    private FrameCodec $codec;
    private RuntimeConnection $runtimeConnection;
    private ConsumerRegistry $consumers;
    private int $negotiatedHeartbeat;
    private int $lastReceivedAt;
    private int $lastSentAt;
    private ?AuthenticatedUser $authenticatedUser = null;
    private ?string $virtualHost = null;

    /**
     * @var callable(): int
     */
    private $clock;

    /**
     * @var null|callable(string, string): void
     */
    private $queueDeletionNotifier;

    /**
     * @var array<int, true>
     */
    private array $channels = [];

    /**
     * @var array<int, int>
     */
    private array $prefetchCounts = [];

    /**
     * @var array<int, true>
     */
    private array $confirmChannels = [];

    /**
     * @var array<int, int>
     */
    private array $nextPublishSequenceTags = [];

    /**
     * @var array<int, array{exchange: string, routing_key: string, mandatory: bool, immediate: bool}>
     */
    private array $pendingPublishMethods = [];

    /**
     * @var array<int, array{exchange: string, routing_key: string, mandatory: bool, immediate: bool, body_size: int, properties: array<string, mixed>, body: string}>
     */
    private array $pendingPublishes = [];

    /**
     * @var array<string, array{consumer: RuntimeConsumer, channel: int, queue: string, no_ack: bool, exclusive: bool}>
     */
    private array $activeConsumers = [];

    /**
     * @var array<int, int>
     */
    private array $nextDeliveryTags = [];

    /**
     * @var array<int, array<int, array{delivery: Delivery, consumer_tag: string, queue: string}>>
     */
    private array $unackedDeliveries = [];

    private bool $consumerDeliveryScheduled = false;

    /**
     * @param resource $socket
     */
    public function __construct(
        private mixed $socket,
        private readonly ConnectionRegistry $connections,
        private readonly int $maxFrameSize = 131072,
        private readonly ?Broker $broker = null,
        private readonly ?AuthenticationService $authenticator = null,
        private readonly ?AuthorizationService $authorizer = null,
        ?ConsumerRegistry $consumers = null,
        private readonly int $maxMessageSize = 10485760,
        private readonly int $heartbeatInterval = 60,
        private readonly ?ResourceLimits $limits = null,
        private bool $draining = false,
        ?callable $clock = null,
        ?callable $queueDeletionNotifier = null
    ) {
        if ($this->heartbeatInterval < 0 || $this->heartbeatInterval > 65535) {
            throw new RuntimeException('AMQP heartbeat interval must fit in an unsigned short.');
        }

        stream_set_blocking($this->socket, false);
        stream_set_write_buffer($this->socket, 0);
        $this->codec = new FrameCodec($this->maxFrameSize);
        $this->consumers = $consumers ?? new ConsumerRegistry();
        $this->clock = $clock ?? static fn (): int => hrtime(true);
        $this->queueDeletionNotifier = $queueDeletionNotifier;
        $this->lastReceivedAt = $this->now();
        $this->lastSentAt = $this->lastReceivedAt;
        $this->negotiatedHeartbeat = $this->heartbeatInterval;
        $this->runtimeConnection = RuntimeConnection::create(
            'amqp-0-9-1',
            @stream_socket_get_name($this->socket, true) ?: null,
            ['state' => $this->state->value]
        );
        $this->connections->add($this->runtimeConnection);
    }

    public function tick(): void
    {
        if ($this->state === AmqpConnectionState::Closed) {
            return;
        }

        while (!feof($this->socket)) {
            $bytes = fread($this->socket, 8192);
            if ($bytes === false || $bytes === '') {
                break;
            }

            try {
                $this->receive($bytes);
                if ($this->state === AmqpConnectionState::Closed || !is_resource($this->socket)) {
                    return;
                }
            } catch (ProtocolException) {
                $this->close();
                return;
            }
        }

        if (feof($this->socket)) {
            $this->close();
            return;
        }

        if ($this->isHeartbeatTimedOut()) {
            error_log(sprintf('AMQP connection timed out: %s', $this->runtimeConnection->id));
            $this->close();
            return;
        }

        $this->deliverToConsumers();
        if ($this->state === AmqpConnectionState::Closed) {
            return;
        }

        $this->sendHeartbeatIfIdle();
    }

    public function receive(string $bytes): void
    {
        $receivedAt = $this->now();

        if ($this->state === AmqpConnectionState::AwaitingProtocolHeader) {
            $this->headerBuffer .= $bytes;

            if (strlen($this->headerBuffer) < strlen(self::PROTOCOL_HEADER)) {
                $this->lastReceivedAt = $receivedAt;
                return;
            }

            $header = substr($this->headerBuffer, 0, strlen(self::PROTOCOL_HEADER));
            if ($header !== self::PROTOCOL_HEADER) {
                throw new ProtocolException('Unsupported AMQP protocol header.');
            }

            $remaining = substr($this->headerBuffer, strlen(self::PROTOCOL_HEADER));
            $this->headerBuffer = '';
            $this->state = AmqpConnectionState::Starting;
            $this->lastReceivedAt = $receivedAt;
            $this->writeFrame($this->connectionStart());

            if ($remaining === '') {
                return;
            }

            $bytes = $remaining;
        }

        foreach ($this->codec->push($bytes) as $frame) {
            $this->handleFrame($frame);
            $this->lastReceivedAt = $receivedAt;
        }

        $this->flushScheduledConsumerDeliveries();
    }

    public function close(): void
    {
        if ($this->state === AmqpConnectionState::Closed) {
            return;
        }

        $this->state = AmqpConnectionState::Closing;
        $this->releaseOutstandingDeliveries();
        foreach (array_keys($this->activeConsumers) as $consumerTag) {
            $this->removeConsumer($consumerTag);
        }
        if ($this->broker !== null) {
            try {
                $this->broker->deleteExclusiveQueuesForConnection($this->runtimeConnection->id);
            } catch (RuntimeException $exception) {
                error_log(sprintf(
                    'AMQP exclusive queue cleanup failed for connection %s: %s',
                    $this->runtimeConnection->id,
                    $exception->getMessage()
                ));
            }
        }

        if (is_resource($this->socket)) {
            fclose($this->socket);
        }

        $this->connections->remove($this->runtimeConnection->id);
        $this->channels = [];
        $this->prefetchCounts = [];
        $this->confirmChannels = [];
        $this->nextPublishSequenceTags = [];
        $this->pendingPublishMethods = [];
        $this->pendingPublishes = [];
        $this->nextDeliveryTags = [];
        error_log(sprintf('AMQP connection closed: %s', $this->runtimeConnection->id));
        $this->state = AmqpConnectionState::Closed;
    }

    public function beginDrain(): void
    {
        $this->draining = true;
    }

    public function unacknowledgedDeliveryCount(): int
    {
        $count = 0;
        foreach ($this->unackedDeliveries as $deliveries) {
            $count += count($deliveries);
        }

        return $count;
    }

    public function cancelConsumersForDeletedQueue(string $virtualHost, string $queue): int
    {
        if ($this->state === AmqpConnectionState::Closed) {
            return 0;
        }

        return $this->cancelLocalConsumersForDeletedQueue($virtualHost, $queue);
    }

    public function state(): AmqpConnectionState
    {
        return $this->state;
    }

    public function isClosed(): bool
    {
        return $this->state === AmqpConnectionState::Closed;
    }

    public function negotiatedHeartbeatInterval(): int
    {
        return $this->negotiatedHeartbeat;
    }

    private function handleFrame(Frame $frame): void
    {
        if ($frame->type === Frame::TYPE_HEARTBEAT) {
            if ($frame->channel !== 0 || $frame->payload !== '') {
                throw new ProtocolException('AMQP heartbeat frames must use channel 0 with an empty payload.');
            }

            return;
        }

        if ($frame->type === Frame::TYPE_HEADER || $frame->type === Frame::TYPE_BODY) {
            $this->handleContentFrame($frame);
            return;
        }

        [$classId, $methodId] = $frame->method();

        if ($this->state === AmqpConnectionState::Open && $frame->channel === 0) {
            $this->handleOpenConnectionControlFrame($classId, $methodId);
            return;
        }

        if ($this->state === AmqpConnectionState::Open) {
            $this->handleOpenConnectionFrame($frame, $classId, $methodId);
            return;
        }

        if ($frame->channel !== 0) {
            throw new ProtocolException('AMQP connection handshake must use channel 0.');
        }

        if ($this->state === AmqpConnectionState::Starting && $classId === 10 && $methodId === 11) {
            if (!$this->handleStartOk($frame)) {
                return;
            }
            $this->state = AmqpConnectionState::Tuning;
            $this->writeFrame($this->connectionTune());
            return;
        }

        if ($this->state === AmqpConnectionState::Tuning && $classId === 10 && $methodId === 31) {
            $this->negotiateHeartbeat($frame);
            $this->state = AmqpConnectionState::Opening;
            return;
        }

        if ($this->state === AmqpConnectionState::Opening && $classId === 10 && $methodId === 40) {
            if (!$this->handleConnectionOpen($frame)) {
                return;
            }
            $this->state = AmqpConnectionState::Open;
            $this->writeFrame($this->connectionOpenOk());
            return;
        }

        throw new ProtocolException('Unexpected AMQP method for current connection state.');
    }

    private function handleOpenConnectionControlFrame(int $classId, int $methodId): void
    {
        if ($classId === 10 && $methodId === 50) {
            $this->writeFrame(Frame::methodFrame(0, 10, 51));
            $this->close();
            return;
        }

        if ($classId === 10 && $methodId === 51) {
            $this->close();
            return;
        }

        throw new ProtocolException('Unexpected AMQP connection method for open connection.');
    }

    private function handleOpenConnectionFrame(Frame $frame, int $classId, int $methodId): void
    {
        if ($classId === 20 && $methodId === 10) {
            $limits = $this->limits ?? new ResourceLimits();
            if (!$limits->allows($limits->maxChannelsPerConnection, count($this->channels))) {
                $this->sendChannelError($frame->channel, 506, 'RESOURCE_ERROR - channel limit reached', 20, 10);
                return;
            }

            $this->channels[$frame->channel] = true;
            $this->writeFrame(Frame::methodFrame($frame->channel, 20, 11, $this->longString('')));
            return;
        }

        if ($classId === 20 && $methodId === 40) {
            $this->closeChannel($frame->channel);
            $this->writeFrame(Frame::methodFrame($frame->channel, 20, 41));
            return;
        }

        if ($classId === 20 && $methodId === 41) {
            $this->closeChannel($frame->channel);
            return;
        }

        if (!isset($this->channels[$frame->channel])) {
            $this->sendChannelError($frame->channel, 504, 'CHANNEL_ERROR - channel is not open', $classId, $methodId);
            return;
        }

        try {
            match ([$classId, $methodId]) {
                [50, 10] => $this->handleQueueDeclare($frame),
                [40, 10] => $this->handleExchangeDeclare($frame),
                [50, 20] => $this->handleQueueBind($frame),
                [50, 30] => $this->handleQueuePurge($frame),
                [50, 40] => $this->handleQueueDelete($frame),
                [50, 50] => $this->handleQueueUnbind($frame),
                [40, 20] => $this->handleExchangeDelete($frame),
                [85, 10] => $this->handleConfirmSelect($frame),
                [60, 10] => $this->handleBasicQos($frame),
                [60, 40] => $this->handleBasicPublish($frame),
                [60, 20] => $this->handleBasicConsume($frame),
                [60, 30] => $this->handleBasicCancel($frame),
                [60, 70] => $this->handleBasicGet($frame),
                [60, 80] => $this->handleBasicAck($frame),
                [60, 90] => $this->handleBasicReject($frame),
                [60, 120] => $this->handleBasicNack($frame),
                default => $this->sendChannelError(
                    $frame->channel,
                    540,
                    'NOT_IMPLEMENTED - AMQP method is not supported by FluxQ yet',
                    $classId,
                    $methodId
                ),
            };
        } catch (TopologyException $exception) {
            $this->sendChannelError(
                $frame->channel,
                $this->replyCodeForTopologyException($exception),
                $exception->getMessage(),
                $classId,
                $methodId
            );
        } catch (ResourceLimitException $exception) {
            $this->sendChannelError($frame->channel, 506, $exception->getMessage(), $classId, $methodId);
        } catch (RuntimeException $exception) {
            $this->sendChannelError($frame->channel, 541, $exception->getMessage(), $classId, $methodId);
        }
    }

    private function handleStartOk(Frame $frame): bool
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->skipTable();
        $mechanism = $reader->readShortString();
        $response = $reader->readLongString();
        $reader->readShortString();
        $reader->assertComplete();

        if ($mechanism !== 'PLAIN') {
            $this->sendConnectionClose(503, 'COMMAND_INVALID - unsupported SASL mechanism', 10, 11);
            return false;
        }

        $credentials = $this->parsePlainResponse($response);
        if ($credentials === null) {
            $this->sendConnectionClose(501, 'FRAME_ERROR - malformed SASL response', 10, 11);
            return false;
        }

        [$authzid, $username, $password] = $credentials;
        if ($authzid !== '' && $authzid !== $username) {
            $this->sendConnectionClose(403, 'ACCESS_REFUSED - authorization identity is not supported', 10, 11);
            return false;
        }

        $result = $this->authenticator()->authenticate($username, $password);
        if (!$result->authenticated || $result->user === null) {
            $this->sendConnectionClose(403, 'ACCESS_REFUSED - authentication failed', 10, 11);
            return false;
        }

        $this->authenticatedUser = $result->user;

        return true;
    }

    private function handleConnectionOpen(Frame $frame): bool
    {
        if ($this->authenticatedUser === null) {
            $this->sendConnectionClose(403, 'ACCESS_REFUSED - authentication required', 10, 40);
            return false;
        }

        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $virtualHost = $reader->readShortString();
        $reader->readShortString();
        $reader->readOctet();
        $reader->assertComplete();

        if ($virtualHost === '') {
            $this->sendConnectionClose(530, 'NOT_ALLOWED - virtual host is required', 10, 40);
            return false;
        }

        if (!$this->authenticator()->canAccessVirtualHost($this->authenticatedUser, $virtualHost)) {
            $this->sendConnectionClose(530, 'NOT_ALLOWED - virtual host access refused', 10, 40);
            return false;
        }

        $this->virtualHost = $virtualHost;

        return true;
    }

    /**
     * @return null|array{0: string, 1: string, 2: string}
     */
    private function parsePlainResponse(string $response): ?array
    {
        $parts = explode("\0", $response);
        if (count($parts) !== 3 || $parts[1] === '') {
            return null;
        }

        return [$parts[0], $parts[1], $parts[2]];
    }

    private function handleBasicQos(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $prefetchSize = $reader->readLong();
        $prefetchCount = $reader->readShort();
        $bits = $reader->readOctet();
        $reader->assertComplete();

        if ($prefetchSize !== 0) {
            $this->sendChannelError($frame->channel, 540, 'NOT_IMPLEMENTED - basic.qos prefetch-size is not supported', 60, 10);
            return;
        }

        if (($bits & 0b00000001) !== 0) {
            $this->sendChannelError($frame->channel, 540, 'NOT_IMPLEMENTED - basic.qos global=true is not supported', 60, 10);
            return;
        }

        $this->prefetchCounts[$frame->channel] = $prefetchCount;
        $this->writeFrame(Frame::methodFrame($frame->channel, 60, 11));
        $this->scheduleConsumerDelivery();
    }

    private function handleConfirmSelect(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $bits = $reader->readOctet();
        $reader->assertComplete();

        $this->confirmChannels[$frame->channel] = true;
        $this->nextPublishSequenceTags[$frame->channel] ??= 1;

        if (($bits & 0b00000001) === 0) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 85, 11));
        }
    }

    private function handleQueueDeclare(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $queue = $reader->readShortString();
        $bits = $reader->readOctet();
        $passive = ($bits & 0b00000001) !== 0;
        $durable = ($bits & 0b00000010) !== 0;
        $exclusive = ($bits & 0b00000100) !== 0;
        $autoDelete = ($bits & 0b00001000) !== 0;
        $noWait = ($bits & 0b00010000) !== 0;
        $reader->skipTable();
        $reader->assertComplete();

        if (!$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $queue, 50, 10)) {
            return;
        }

        if ($passive) {
            $status = $this->broker()->queueStatus($this->openedVirtualHost(), $queue, $this->runtimeConnection->id);
            $destination = $status->destination;
            $messageCount = $status->messageCount;
        } else {
            $destination = $this->broker()->declareQueue(
                $this->openedVirtualHost(),
                $queue,
                $durable,
                $autoDelete,
                exclusive: $exclusive,
                connectionId: $this->runtimeConnection->id
            );
            $messageCount = $this->broker()->readyMessageCount($destination);
        }

        if (!$noWait) {
            $consumerCount = $this->consumers->countByDestination($this->openedVirtualHost(), $destination->name);
            $this->writeFrame(Frame::methodFrame(
                $frame->channel,
                50,
                11,
                $this->shortString($destination->name) . pack('NN', $messageCount, $consumerCount)
            ));
        }
    }

    private function handleExchangeDeclare(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $exchange = $reader->readShortString();
        $type = $reader->readShortString();
        $bits = $reader->readOctet();
        $passive = ($bits & 0b00000001) !== 0;
        $durable = ($bits & 0b00000010) !== 0;
        $autoDelete = ($bits & 0b00000100) !== 0;
        $internal = ($bits & 0b00001000) !== 0;
        $noWait = ($bits & 0b00010000) !== 0;
        $reader->skipTable();
        $reader->assertComplete();

        if ($exchange === '') {
            throw new TopologyException('The default AMQP exchange is implicit.', TopologyException::PRECONDITION_FAILED);
        }

        $sourceType = RoutingSourceType::tryFrom($type);
        if (
            $sourceType === null
            || !in_array($sourceType, [RoutingSourceType::Direct, RoutingSourceType::Fanout, RoutingSourceType::Topic], true)
        ) {
            throw new TopologyException(sprintf('Exchange type "%s" is not supported.', $type), TopologyException::NOT_IMPLEMENTED);
        }

        if (!$passive && $internal) {
            throw new TopologyException('Internal exchanges are not supported yet.', TopologyException::NOT_IMPLEMENTED);
        }

        if (!$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $exchange, 40, 10)) {
            return;
        }

        if ($passive) {
            $this->broker()->routingSourceStatus($this->openedVirtualHost(), $exchange, $sourceType);
        } else {
            match ($sourceType) {
                RoutingSourceType::Direct => $this->broker()->declareDirectRoutingSource(
                    $this->openedVirtualHost(),
                    $exchange,
                    $durable,
                    $autoDelete
                ),
                RoutingSourceType::Fanout => $this->broker()->declareFanoutRoutingSource(
                    $this->openedVirtualHost(),
                    $exchange,
                    $durable,
                    $autoDelete
                ),
                RoutingSourceType::Topic => $this->broker()->declareTopicRoutingSource(
                    $this->openedVirtualHost(),
                    $exchange,
                    $durable,
                    $autoDelete
                ),
            };
        }

        if (!$noWait) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 40, 11));
        }
    }

    private function handleQueueBind(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $queue = $reader->readShortString();
        $exchange = $reader->readShortString();
        $routingKey = $reader->readShortString();
        $bits = $reader->readOctet();
        $noWait = ($bits & 0b00000001) !== 0;
        $reader->skipTable();
        $reader->assertComplete();

        if (
            !$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $queue, 50, 20)
            || ($exchange !== '' && !$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $exchange, 50, 20))
        ) {
            return;
        }

        $this->broker()->bindQueue($this->openedVirtualHost(), $exchange, $queue, $routingKey, $this->runtimeConnection->id);

        if (!$noWait) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 50, 21));
        }
    }

    private function handleQueuePurge(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $queue = $reader->readShortString();
        $bits = $reader->readOctet();
        $noWait = ($bits & 0b00000001) !== 0;
        $reader->assertComplete();

        if (
            !$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $queue, 50, 30)
            || !$this->authorizeResource($frame->channel, AuthorizationPermission::Write, $queue, 50, 30)
        ) {
            return;
        }

        $messageCount = $this->broker()->purgeQueue($this->openedVirtualHost(), $queue, $this->runtimeConnection->id);
        $this->clearUnackedForQueue($queue);

        if (!$noWait) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 50, 31, pack('N', $messageCount)));
        }
    }

    private function handleQueueDelete(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $queue = $reader->readShortString();
        $bits = $reader->readOctet();
        $ifUnused = ($bits & 0b00000001) !== 0;
        $ifEmpty = ($bits & 0b00000010) !== 0;
        $noWait = ($bits & 0b00000100) !== 0;
        $reader->assertComplete();

        if (!$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $queue, 50, 40)) {
            return;
        }

        $virtualHost = $this->openedVirtualHost();
        if ($ifUnused && $this->consumers->countByDestination($virtualHost, $queue) > 0) {
            throw new TopologyException(sprintf('Queue "%s" is in use.', $queue), TopologyException::PRECONDITION_FAILED);
        }

        try {
            $this->broker()->assertQueueDeletable($virtualHost, $queue, $ifEmpty, $this->runtimeConnection->id);
        } catch (TopologyException $exception) {
            if ($exception->reason !== TopologyException::NOT_FOUND) {
                throw $exception;
            }

            if (!$noWait) {
                $this->writeFrame(Frame::methodFrame($frame->channel, 50, 41, pack('N', 0)));
            }

            return;
        }

        $this->notifyQueueDeleted($virtualHost, $queue);
        $messageCount = $this->broker()->deleteQueue($virtualHost, $queue, $ifEmpty, $this->runtimeConnection->id);

        if (!$noWait) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 50, 41, pack('N', $messageCount)));
        }
    }

    private function handleQueueUnbind(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $queue = $reader->readShortString();
        $exchange = $reader->readShortString();
        $routingKey = $reader->readShortString();
        $reader->skipTable();
        $reader->assertComplete();

        if (
            !$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $queue, 50, 50)
            || ($exchange !== '' && !$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $exchange, 50, 50))
        ) {
            return;
        }

        $this->broker()->unbindQueue($this->openedVirtualHost(), $exchange, $queue, $routingKey, $this->runtimeConnection->id);
        $this->writeFrame(Frame::methodFrame($frame->channel, 50, 51));
    }

    private function handleExchangeDelete(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $exchange = $reader->readShortString();
        $bits = $reader->readOctet();
        $ifUnused = ($bits & 0b00000001) !== 0;
        $noWait = ($bits & 0b00000010) !== 0;
        $reader->assertComplete();

        if (!$this->authorizeResource($frame->channel, AuthorizationPermission::Configure, $exchange, 40, 20)) {
            return;
        }

        $this->broker()->deleteRoutingSource($this->openedVirtualHost(), $exchange, $ifUnused);

        if (!$noWait) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 40, 21));
        }
    }

    private function handleBasicPublish(Frame $frame): void
    {
        if ($this->draining) {
            $this->sendDrainChannelError($frame->channel, 60, 40);
            return;
        }

        if (isset($this->pendingPublishMethods[$frame->channel]) || isset($this->pendingPublishes[$frame->channel])) {
            throw new ProtocolException('AMQP publish content sequence is already in progress.');
        }

        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $exchange = $reader->readShortString();
        $routingKey = $reader->readShortString();
        $bits = $reader->readOctet();
        $reader->assertComplete();

        $resource = $exchange === '' ? $routingKey : $exchange;
        if (!$this->authorizeResource($frame->channel, AuthorizationPermission::Write, $resource, 60, 40)) {
            return;
        }

        $this->pendingPublishMethods[$frame->channel] = [
            'exchange' => $exchange,
            'routing_key' => $routingKey,
            'mandatory' => ($bits & 0b00000001) !== 0,
            'immediate' => ($bits & 0b00000010) !== 0,
        ];
    }

    private function handleBasicConsume(Frame $frame): void
    {
        if ($this->draining) {
            $this->sendDrainChannelError($frame->channel, 60, 20);
            return;
        }

        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $queue = $reader->readShortString();
        $consumerTag = $reader->readShortString();
        $bits = $reader->readOctet();
        $noLocal = ($bits & 0b00000001) !== 0;
        $noAck = ($bits & 0b00000010) !== 0;
        $exclusive = ($bits & 0b00000100) !== 0;
        $noWait = ($bits & 0b00001000) !== 0;
        $reader->skipTable();
        $reader->assertComplete();

        if ($queue === '') {
            throw new TopologyException('Consumer queue must not be empty.', TopologyException::PRECONDITION_FAILED);
        }

        if ($noLocal) {
            throw new TopologyException('no-local consumers are not supported yet.', TopologyException::NOT_IMPLEMENTED);
        }

        if ($consumerTag === '') {
            $consumerTag = sprintf('ctag-%s-%d', $this->runtimeConnection->id, count($this->activeConsumers) + 1);
        }

        if (isset($this->activeConsumers[$consumerTag])) {
            throw new TopologyException(sprintf('Consumer tag "%s" is already active.', $consumerTag), TopologyException::PRECONDITION_FAILED);
        }

        $limits = $this->limits ?? new ResourceLimits();
        if (!$limits->allows($limits->maxConsumersPerConnection, count($this->activeConsumers))) {
            throw new ResourceLimitException('Consumer limit reached for AMQP connection.');
        }

        if (!$limits->allows($limits->maxConsumersPerChannel, $this->consumerCountForChannel($frame->channel))) {
            throw new ResourceLimitException('Consumer limit reached for AMQP channel.');
        }

        if (!$this->authorizeResource($frame->channel, AuthorizationPermission::Read, $queue, 60, 20)) {
            return;
        }

        $virtualHost = $this->openedVirtualHost();
        $this->broker()->ensureQueueSubscription($virtualHost, $queue, 'amqp', $this->runtimeConnection->id);
        if (!$this->consumers->canRegisterConsumer($virtualHost, $queue, $exclusive)) {
            throw new TopologyException(
                sprintf('ACCESS_REFUSED - queue "%s" already has an active consumer', $queue),
                TopologyException::ACCESS_REFUSED
            );
        }

        $consumer = RuntimeConsumer::create(
            $this->runtimeConnection->id,
            $virtualHost,
            $queue,
            'amqp',
            $exclusive,
            ['protocol' => 'amqp-0-9-1', 'channel' => $frame->channel, 'consumer_tag' => $consumerTag]
        );
        $this->consumers->add($consumer);
        $this->activeConsumers[$consumerTag] = [
            'consumer' => $consumer,
            'channel' => $frame->channel,
            'queue' => $queue,
            'no_ack' => $noAck,
            'exclusive' => $exclusive,
        ];

        if (!$noWait) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 60, 21, $this->shortString($consumerTag)));
        }

        $this->scheduleConsumerDelivery();
    }

    private function handleBasicCancel(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $consumerTag = $reader->readShortString();
        $bits = $reader->readOctet();
        $reader->assertComplete();

        $state = $this->activeConsumers[$consumerTag] ?? null;
        if ($state === null || $state['channel'] !== $frame->channel) {
            $this->sendChannelError(
                $frame->channel,
                404,
                sprintf('NOT_FOUND - consumer tag "%s" is not active on this channel', $consumerTag),
                60,
                30
            );
            return;
        }

        $queue = $state['queue'];
        $this->removeConsumer($consumerTag, deleteAutoDeleteQueue: false);
        $this->deleteAutoDeleteQueueAfterFinalConsumer($this->openedVirtualHost(), $queue);

        if (($bits & 0b00000001) === 0) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 60, 31, $this->shortString($consumerTag)));
        }
    }

    private function handleBasicGet(Frame $frame): void
    {
        if ($this->draining) {
            $this->sendDrainChannelError($frame->channel, 60, 70);
            return;
        }

        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();
        $queue = $reader->readShortString();
        $bits = $reader->readOctet();
        $noAck = ($bits & 0b00000001) !== 0;
        $reader->assertComplete();

        if (!$this->authorizeResource($frame->channel, AuthorizationPermission::Read, $queue, 60, 70)) {
            return;
        }

        $this->broker()->ensureQueueSubscription($this->openedVirtualHost(), $queue, 'amqp', $this->runtimeConnection->id);
        $deliveryTag = $this->nextDeliveryTags[$frame->channel] ?? 1;
        $delivery = $this->broker()->reserve(new ReserveRequest(
            $this->openedVirtualHost(),
            $queue,
            'amqp',
            $this->runtimeConnection->id,
            (string) $deliveryTag,
            $this->runtimeConnection->id
        ));

        if ($delivery === null) {
            $this->writeFrame(Frame::methodFrame($frame->channel, 60, 72, $this->shortString('')));
            return;
        }

        $message = $this->broker()->messageForDelivery($delivery);
        $this->writeFrame(Frame::methodFrame(
            $frame->channel,
            60,
            71,
            $this->packLongLong($deliveryTag)
                . chr($delivery->attempts > 1 ? 1 : 0)
                . $this->shortString('')
                . $this->shortString($queue)
                . pack('N', 0)
        ));
        $this->writeFrame(new Frame(Frame::TYPE_HEADER, $frame->channel, $this->contentHeader($message)));
        foreach (str_split($message->payload, $this->outboundBodyChunkSize()) as $chunk) {
            $this->writeFrame(new Frame(Frame::TYPE_BODY, $frame->channel, $chunk));
        }

        $this->nextDeliveryTags[$frame->channel] = $deliveryTag + 1;
        if ($noAck) {
            $this->broker()->acknowledge(new AcknowledgeRequest($delivery->id));
        } else {
            $this->unackedDeliveries[$frame->channel][$deliveryTag] = [
                'delivery' => $delivery,
                'consumer_tag' => '',
                'queue' => $queue,
            ];
        }
    }

    private function handleBasicAck(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $deliveryTag = $reader->readLongLong();
        $bits = $reader->readOctet();
        $reader->assertComplete();
        $multiple = ($bits & 0b00000001) !== 0;

        $settlements = $this->settlementsForDeliveryTag($frame->channel, $deliveryTag, $multiple);
        if ($settlements === []) {
            $this->sendChannelError($frame->channel, 406, 'PRECONDITION_FAILED - unknown delivery tag', 60, 80);
            return;
        }

        foreach ($settlements as $mapping) {
            $this->broker()->acknowledge(new AcknowledgeRequest($mapping['delivery']->id));
        }

        foreach (array_keys($settlements) as $settledDeliveryTag) {
            $this->forgetUnackedDelivery($frame->channel, (int) $settledDeliveryTag);
        }

        $this->scheduleConsumerDelivery();
    }

    private function handleBasicReject(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $deliveryTag = $reader->readLongLong();
        $bits = $reader->readOctet();
        $reader->assertComplete();

        $mapping = $this->unackedDeliveries[$frame->channel][$deliveryTag] ?? null;
        if ($mapping === null) {
            $this->sendChannelError($frame->channel, 406, 'PRECONDITION_FAILED - unknown delivery tag', 60, 90);
            return;
        }

        if (($bits & 0b00000001) !== 0) {
            $this->broker()->release(new ReleaseRequest($mapping['delivery']->id));
        } else {
            $this->broker()->reject(new RejectRequest($mapping['delivery']->id));
        }

        $this->forgetUnackedDelivery($frame->channel, $deliveryTag);
        $this->scheduleConsumerDelivery();
    }

    private function handleBasicNack(Frame $frame): void
    {
        $reader = new AmqpMethodReader(substr($frame->payload, 4));
        $deliveryTag = $reader->readLongLong();
        $bits = $reader->readOctet();
        $reader->assertComplete();
        $multiple = ($bits & 0b00000001) !== 0;

        $settlements = $this->settlementsForDeliveryTag($frame->channel, $deliveryTag, $multiple);
        if ($settlements === []) {
            $this->sendChannelError($frame->channel, 406, 'PRECONDITION_FAILED - unknown delivery tag', 60, 120);
            return;
        }

        foreach ($settlements as $mapping) {
            if (($bits & 0b00000010) !== 0) {
                $this->broker()->release(new ReleaseRequest($mapping['delivery']->id));
            } else {
                $this->broker()->reject(new RejectRequest($mapping['delivery']->id));
            }
        }

        foreach (array_keys($settlements) as $settledDeliveryTag) {
            $this->forgetUnackedDelivery($frame->channel, (int) $settledDeliveryTag);
        }

        $this->scheduleConsumerDelivery();
    }

    /**
     * @return array<int, array{delivery: Delivery, consumer_tag: string, queue: string}>
     */
    private function settlementsForDeliveryTag(int $channel, int $deliveryTag, bool $multiple): array
    {
        if (!$multiple) {
            if ($deliveryTag === 0) {
                return [];
            }

            $mapping = $this->unackedDeliveries[$channel][$deliveryTag] ?? null;

            return $mapping === null ? [] : [$deliveryTag => $mapping];
        }

        $deliveries = $this->unackedDeliveries[$channel] ?? [];
        if ($deliveries === []) {
            return [];
        }

        if ($deliveryTag === 0) {
            ksort($deliveries);

            return $deliveries;
        }

        if (!isset($deliveries[$deliveryTag])) {
            return [];
        }

        $settlements = [];
        foreach ($deliveries as $outstandingDeliveryTag => $mapping) {
            if ($outstandingDeliveryTag <= $deliveryTag) {
                $settlements[$outstandingDeliveryTag] = $mapping;
            }
        }
        ksort($settlements);

        return $settlements;
    }

    private function handleContentFrame(Frame $frame): void
    {
        if ($this->state !== AmqpConnectionState::Open || !isset($this->channels[$frame->channel])) {
            throw new ProtocolException('AMQP content frame arrived on an invalid channel.');
        }

        if ($frame->type === Frame::TYPE_HEADER) {
            $this->handleContentHeader($frame);
            return;
        }

        $this->handleContentBody($frame);
    }

    private function handleContentHeader(Frame $frame): void
    {
        $method = $this->pendingPublishMethods[$frame->channel] ?? null;
        if ($method === null || isset($this->pendingPublishes[$frame->channel])) {
            throw new ProtocolException('AMQP content header arrived without a pending publish.');
        }

        $reader = new AmqpMethodReader($frame->payload);
        $classId = $reader->readShort();
        $reader->readShort();
        $bodySize = $reader->readLongLong();
        if ($classId !== 60) {
            throw new ProtocolException('AMQP content header class is not supported.');
        }

        if ($this->maxMessageSize !== 0 && $bodySize > $this->maxMessageSize) {
            throw new ProtocolException('AMQP message body exceeds configured message size limit.');
        }

        $properties = $this->readBasicProperties($reader);
        $reader->assertComplete();
        unset($this->pendingPublishMethods[$frame->channel]);
        $this->pendingPublishes[$frame->channel] = [
            'exchange' => $method['exchange'],
            'routing_key' => $method['routing_key'],
            'mandatory' => $method['mandatory'],
            'immediate' => $method['immediate'],
            'body_size' => $bodySize,
            'properties' => $properties,
            'body' => '',
        ];

        if ($bodySize === 0) {
            $this->completePublish($frame->channel);
        }
    }

    private function handleContentBody(Frame $frame): void
    {
        if (!isset($this->pendingPublishes[$frame->channel])) {
            throw new ProtocolException('AMQP content body arrived without a content header.');
        }

        $publish = &$this->pendingPublishes[$frame->channel];
        $newSize = strlen($publish['body']) + strlen($frame->payload);
        if ($newSize > $publish['body_size'] || ($this->maxMessageSize !== 0 && $newSize > $this->maxMessageSize)) {
            throw new ProtocolException('AMQP content body byte count exceeds declared size.');
        }

        $publish['body'] .= $frame->payload;
        if (strlen($publish['body']) === $publish['body_size']) {
            unset($publish);
            $this->completePublish($frame->channel);
        }
    }

    private function completePublish(int $channel): void
    {
        $publish = $this->pendingPublishes[$channel] ?? throw new ProtocolException('No AMQP publish content is pending.');
        unset($this->pendingPublishes[$channel]);

        $properties = $publish['properties'];
        $metadata = $this->amqpMessageMetadata($properties);

        if ($publish['exchange'] === '') {
            try {
                $this->broker()->publishToDefaultExchange(
                    $this->openedVirtualHost(),
                    $publish['routing_key'],
                    $publish['body'],
                    $properties['headers'] ?? [],
                    $properties['content_type'] ?? null,
                    $properties['content_encoding'] ?? null,
                    $properties['priority'] ?? 0,
                    ($properties['delivery_mode'] ?? 2) === 2,
                    metadata: $metadata,
                    connectionId: $this->runtimeConnection->id
                );
                $this->sendPublishConfirm($channel);
                $this->deliverToConsumers();
            } catch (TopologyException $exception) {
                $this->sendChannelError($channel, $this->replyCodeForTopologyException($exception), $exception->getMessage(), 60, 40);
            } catch (ResourceLimitException $exception) {
                $this->sendChannelError($channel, 506, $exception->getMessage(), 60, 40);
            } catch (Throwable $exception) {
                $this->sendChannelError($channel, 541, $exception->getMessage(), 60, 40);
            }

            return;
        }

        try {
            $result = $this->broker()->publish(new PublishRequest(
                $this->openedVirtualHost(),
                $publish['exchange'],
                $publish['routing_key'],
                $publish['body'],
                $properties['headers'] ?? [],
                $properties['content_type'] ?? null,
                $properties['content_encoding'] ?? null,
                $properties['priority'] ?? 0,
                ($properties['delivery_mode'] ?? 2) === 2,
                metadata: $metadata,
                persistUnrouted: false
            ));
            if ($result->routeCount() === 0 && $publish['mandatory']) {
                $this->sendBasicReturn(
                    $channel,
                    312,
                    sprintf(
                        'NO_ROUTE - no route for exchange "%s" with routing key "%s"',
                        $publish['exchange'],
                        $publish['routing_key']
                    ),
                    $publish['exchange'],
                    $publish['routing_key'],
                    $publish['body'],
                    $properties
                );
            }
            $this->sendPublishConfirm($channel);
            $this->deliverToConsumers();
        } catch (TopologyException $exception) {
            $this->sendChannelError($channel, $this->replyCodeForTopologyException($exception), $exception->getMessage(), 60, 40);
        } catch (ResourceLimitException $exception) {
            $this->sendChannelError($channel, 506, $exception->getMessage(), 60, 40);
        } catch (Throwable $exception) {
            $this->sendChannelError($channel, 541, $exception->getMessage(), 60, 40);
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function readBasicProperties(AmqpMethodReader $reader): array
    {
        $flags = [];
        do {
            $flagWord = $reader->readShort();
            $flags[] = $flagWord;
        } while (($flagWord & 1) !== 0);

        $first = $flags[0] ?? 0;
        $properties = [];

        if (($first & 0b1000000000000000) !== 0) {
            $properties['content_type'] = $reader->readShortString();
        }
        if (($first & 0b0100000000000000) !== 0) {
            $properties['content_encoding'] = $reader->readShortString();
        }
        if (($first & 0b0010000000000000) !== 0) {
            $properties['headers'] = $reader->readTable();
        }
        if (($first & 0b0001000000000000) !== 0) {
            $properties['delivery_mode'] = $reader->readOctet();
        }
        if (($first & 0b0000100000000000) !== 0) {
            $properties['priority'] = $reader->readOctet();
        }
        if (($first & 0b0000010000000000) !== 0) {
            $properties['correlation_id'] = $reader->readShortString();
        }
        if (($first & 0b0000001000000000) !== 0) {
            $properties['reply_to'] = $reader->readShortString();
        }
        if (($first & 0b0000000100000000) !== 0) {
            $properties['expiration'] = $reader->readShortString();
        }
        if (($first & 0b0000000010000000) !== 0) {
            $properties['message_id'] = $reader->readShortString();
        }
        if (($first & 0b0000000001000000) !== 0) {
            $properties['timestamp'] = $reader->readLongLong();
        }
        if (($first & 0b0000000000100000) !== 0) {
            $properties['type'] = $reader->readShortString();
        }
        if (($first & 0b0000000000010000) !== 0) {
            $properties['user_id'] = $reader->readShortString();
        }
        if (($first & 0b0000000000001000) !== 0) {
            $properties['app_id'] = $reader->readShortString();
        }
        if (($first & 0b0000000000000100) !== 0) {
            $properties['cluster_id'] = $reader->readShortString();
        }

        return $properties;
    }

    /**
     * @param array<string, mixed> $properties
     * @return array<string, array<string, mixed>>
     */
    private function amqpMessageMetadata(array $properties): array
    {
        return ['amqp_basic_properties' => $properties];
    }

    private function deliverToConsumers(): void
    {
        if ($this->draining) {
            return;
        }

        foreach ($this->activeConsumers as $consumerTag => $state) {
            $channel = $state['channel'];
            if (!isset($this->channels[$channel])) {
                continue;
            }

            if (!$this->consumerHasPrefetchCapacity($channel, $consumerTag)) {
                continue;
            }

            $deliveryTag = $this->nextDeliveryTags[$channel] ?? 1;
            $delivery = $this->broker()->reserve(new ReserveRequest(
                $state['consumer']->virtualHost,
                $state['queue'],
                $state['consumer']->subscription,
                $state['consumer']->id,
                (string) $deliveryTag,
                $this->runtimeConnection->id
            ));

            if ($delivery === null) {
                continue;
            }

            $message = $this->broker()->messageForDelivery($delivery);
            $this->sendDelivery($channel, $consumerTag, $deliveryTag, $delivery, $message, $state['queue']);
            $this->nextDeliveryTags[$channel] = $deliveryTag + 1;

            if ($state['no_ack']) {
                $this->broker()->acknowledge(new AcknowledgeRequest($delivery->id));
            } else {
            $this->unackedDeliveries[$channel][$deliveryTag] = [
                'delivery' => $delivery,
                'consumer_tag' => $consumerTag,
                'queue' => $state['queue'],
            ];
        }
        }
    }

    private function scheduleConsumerDelivery(): void
    {
        $this->consumerDeliveryScheduled = true;
    }

    private function flushScheduledConsumerDeliveries(): void
    {
        if (!$this->consumerDeliveryScheduled) {
            return;
        }

        $this->consumerDeliveryScheduled = false;
        $this->deliverToConsumers();
    }

    private function sendDelivery(
        int $channel,
        string $consumerTag,
        int $deliveryTag,
        Delivery $delivery,
        Message $message,
        string $queue
    ): void {
        $this->writeFrame(Frame::methodFrame(
            $channel,
            60,
            60,
            $this->shortString($consumerTag)
                . $this->packLongLong($deliveryTag)
                . chr($delivery->attempts > 1 ? 1 : 0)
                . $this->shortString('')
                . $this->shortString($queue)
        ));
        $this->writeFrame(new Frame(Frame::TYPE_HEADER, $channel, $this->contentHeader($message)));

        foreach (str_split($message->payload, $this->outboundBodyChunkSize()) as $chunk) {
            $this->writeFrame(new Frame(Frame::TYPE_BODY, $channel, $chunk));
        }
    }

    private function contentHeader(Message $message): string
    {
        $properties = $message->metadata['amqp_basic_properties'] ?? null;
        if (is_array($properties)) {
            return $this->contentHeaderFromProperties($message->payload, $properties);
        }

        $flags = 0;
        $values = '';

        if ($message->contentType !== null) {
            $flags |= 0b1000000000000000;
            $values .= $this->shortString($message->contentType);
        }
        if ($message->contentEncoding !== null) {
            $flags |= 0b0100000000000000;
            $values .= $this->shortString($message->contentEncoding);
        }
        if ($message->headers !== []) {
            $flags |= 0b0010000000000000;
            $values .= $this->fieldTable($message->headers);
        }

        $flags |= 0b0001000000000000;
        $values .= chr($message->persistent ? 2 : 1);
        $flags |= 0b0000100000000000;
        $values .= chr($message->priority);
        $flags |= 0b0000000010000000;
        $values .= $this->shortString($message->messageId);

        return pack('nn', 60, 0) . $this->packLongLong(strlen($message->payload)) . pack('n', $flags) . $values;
    }

    /**
     * @param array<string, mixed> $properties
     */
    private function sendBasicReturn(
        int $channel,
        int $replyCode,
        string $replyText,
        string $exchange,
        string $routingKey,
        string $body,
        array $properties
    ): void {
        $this->writeFrame(Frame::methodFrame(
            $channel,
            60,
            50,
            pack('n', $replyCode)
                . $this->shortString($this->truncateReplyText($replyText))
                . $this->shortString($exchange)
                . $this->shortString($routingKey)
        ));
        $this->writeFrame(new Frame(Frame::TYPE_HEADER, $channel, $this->contentHeaderFromProperties($body, $properties)));
        foreach (str_split($body, $this->outboundBodyChunkSize()) as $chunk) {
            $this->writeFrame(new Frame(Frame::TYPE_BODY, $channel, $chunk));
        }
    }

    /**
     * @param array<string, mixed> $properties
     */
    private function contentHeaderFromProperties(string $body, array $properties): string
    {
        $flags = 0;
        $values = '';

        if (isset($properties['content_type']) && is_string($properties['content_type'])) {
            $flags |= 0b1000000000000000;
            $values .= $this->shortString($properties['content_type']);
        }
        if (isset($properties['content_encoding']) && is_string($properties['content_encoding'])) {
            $flags |= 0b0100000000000000;
            $values .= $this->shortString($properties['content_encoding']);
        }
        if (isset($properties['headers']) && is_array($properties['headers']) && $properties['headers'] !== []) {
            $flags |= 0b0010000000000000;
            $values .= $this->fieldTable($properties['headers']);
        }
        if (isset($properties['delivery_mode']) && is_int($properties['delivery_mode'])) {
            $flags |= 0b0001000000000000;
            $values .= chr($properties['delivery_mode']);
        }
        if (isset($properties['priority']) && is_int($properties['priority'])) {
            $flags |= 0b0000100000000000;
            $values .= chr($properties['priority']);
        }
        if (isset($properties['correlation_id']) && is_string($properties['correlation_id'])) {
            $flags |= 0b0000010000000000;
            $values .= $this->shortString($properties['correlation_id']);
        }
        if (isset($properties['reply_to']) && is_string($properties['reply_to'])) {
            $flags |= 0b0000001000000000;
            $values .= $this->shortString($properties['reply_to']);
        }
        if (isset($properties['expiration']) && is_string($properties['expiration'])) {
            $flags |= 0b0000000100000000;
            $values .= $this->shortString($properties['expiration']);
        }
        if (isset($properties['message_id']) && is_string($properties['message_id'])) {
            $flags |= 0b0000000010000000;
            $values .= $this->shortString($properties['message_id']);
        }
        if (isset($properties['timestamp']) && is_int($properties['timestamp'])) {
            $flags |= 0b0000000001000000;
            $values .= $this->packLongLong($properties['timestamp']);
        }
        if (isset($properties['type']) && is_string($properties['type'])) {
            $flags |= 0b0000000000100000;
            $values .= $this->shortString($properties['type']);
        }
        if (isset($properties['user_id']) && is_string($properties['user_id'])) {
            $flags |= 0b0000000000010000;
            $values .= $this->shortString($properties['user_id']);
        }
        if (isset($properties['app_id']) && is_string($properties['app_id'])) {
            $flags |= 0b0000000000001000;
            $values .= $this->shortString($properties['app_id']);
        }
        if (isset($properties['cluster_id']) && is_string($properties['cluster_id'])) {
            $flags |= 0b0000000000000100;
            $values .= $this->shortString($properties['cluster_id']);
        }

        return pack('nn', 60, 0) . $this->packLongLong(strlen($body)) . pack('n', $flags) . $values;
    }

    private function sendPublishConfirm(int $channel): void
    {
        if (!isset($this->confirmChannels[$channel])) {
            return;
        }

        $deliveryTag = $this->nextPublishSequenceTags[$channel] ?? 1;
        $this->nextPublishSequenceTags[$channel] = $deliveryTag + 1;
        $this->writeFrame(Frame::methodFrame($channel, 60, 80, $this->packLongLong($deliveryTag) . "\x00"));
    }

    private function closeChannel(int $channel): void
    {
        $this->releaseOutstandingDeliveries($channel);

        unset(
            $this->channels[$channel],
            $this->prefetchCounts[$channel],
            $this->confirmChannels[$channel],
            $this->nextPublishSequenceTags[$channel],
            $this->pendingPublishMethods[$channel],
            $this->pendingPublishes[$channel]
        );

        foreach ($this->activeConsumers as $consumerTag => $state) {
            if ($state['channel'] === $channel) {
                $this->removeConsumer($consumerTag);
            }
        }
    }

    private function cancelConsumer(string $consumerTag, bool $deleteAutoDeleteQueue = true): void
    {
        $this->releaseOutstandingDeliveriesForConsumer($consumerTag);
        $this->removeConsumer($consumerTag, $deleteAutoDeleteQueue);
    }

    private function removeConsumer(string $consumerTag, bool $deleteAutoDeleteQueue = true): void
    {
        $state = $this->activeConsumers[$consumerTag] ?? null;
        if ($state === null) {
            return;
        }

        $this->consumers->remove($state['consumer']->id);
        unset($this->activeConsumers[$consumerTag]);

        if ($deleteAutoDeleteQueue) {
            $this->deleteAutoDeleteQueueAfterFinalConsumer($state['consumer']->virtualHost, $state['queue']);
        }
    }

    private function deleteAutoDeleteQueueAfterFinalConsumer(string $virtualHost, string $queue): void
    {
        if (!$this->consumers->hasHadConsumer($virtualHost, $queue)) {
            return;
        }

        if ($this->consumers->countByDestination($virtualHost, $queue) > 0) {
            return;
        }

        try {
            $status = $this->broker()->queueStatus($virtualHost, $queue, $this->runtimeConnection->id);
            if (!$status->destination->autoDelete) {
                return;
            }

            $this->notifyQueueDeleted($virtualHost, $queue);
            $this->broker()->deleteQueue($virtualHost, $queue, connectionId: $this->runtimeConnection->id);
        } catch (TopologyException $exception) {
            if ($exception->reason !== TopologyException::NOT_FOUND) {
                throw $exception;
            }
        }
    }

    private function notifyQueueDeleted(string $virtualHost, string $queue): void
    {
        if ($this->queueDeletionNotifier !== null) {
            ($this->queueDeletionNotifier)($virtualHost, $queue);
            return;
        }

        $this->cancelLocalConsumersForDeletedQueue($virtualHost, $queue);
    }

    private function cancelLocalConsumersForDeletedQueue(string $virtualHost, string $queue): int
    {
        $cancelled = 0;

        foreach (array_keys($this->activeConsumers) as $consumerTag) {
            $state = $this->activeConsumers[$consumerTag] ?? null;
            if ($state === null) {
                continue;
            }

            if ($state['consumer']->virtualHost !== $virtualHost || $state['queue'] !== $queue) {
                continue;
            }

            $this->sendServerBasicCancel($state['channel'], $consumerTag);
            $this->cancelConsumer($consumerTag, deleteAutoDeleteQueue: false);
            $cancelled++;
        }

        return $cancelled;
    }

    private function sendServerBasicCancel(int $channel, string $consumerTag): void
    {
        $this->writeFrame(Frame::methodFrame($channel, 60, 30, $this->shortString($consumerTag) . "\x00"));
    }

    private function clearUnackedForQueue(string $queue): void
    {
        foreach ($this->unackedDeliveries as $channel => $deliveries) {
            foreach ($deliveries as $deliveryTag => $mapping) {
                if ($mapping['queue'] === $queue) {
                    unset($this->unackedDeliveries[$channel][$deliveryTag]);
                }
            }

            $this->clearEmptyUnackedChannel($channel);
        }
    }

    private function releaseOutstandingDeliveries(?int $channel = null): void
    {
        $released = 0;

        foreach ($this->unackedDeliveries as $deliveryChannel => $deliveries) {
            if ($channel !== null && $channel !== $deliveryChannel) {
                continue;
            }

            foreach ($deliveries as $deliveryTag => $mapping) {
                $this->broker()->release(new ReleaseRequest($mapping['delivery']->id));
                $released++;
                $this->forgetUnackedDelivery($deliveryChannel, $deliveryTag);
            }
        }

        if ($released > 0) {
            error_log(sprintf('AMQP unacked deliveries released: %d', $released));
        }
    }

    private function releaseOutstandingDeliveriesForConsumer(string $consumerTag): void
    {
        $released = 0;

        foreach ($this->unackedDeliveries as $channel => $deliveries) {
            foreach ($deliveries as $deliveryTag => $mapping) {
                if ($mapping['consumer_tag'] !== $consumerTag) {
                    continue;
                }

                $this->broker()->release(new ReleaseRequest($mapping['delivery']->id));
                $released++;
                $this->forgetUnackedDelivery($channel, $deliveryTag);
            }
        }

        if ($released > 0) {
            error_log(sprintf('AMQP consumer unacked deliveries released: %d', $released));
        }
    }

    private function consumerHasPrefetchCapacity(int $channel, string $consumerTag): bool
    {
        $prefetchCount = $this->prefetchCounts[$channel] ?? 0;
        if ($prefetchCount === 0) {
            return true;
        }

        return $this->unackedCount($channel, $consumerTag) < $prefetchCount;
    }

    private function unackedCount(int $channel, string $consumerTag): int
    {
        $count = 0;
        foreach ($this->unackedDeliveries[$channel] ?? [] as $mapping) {
            if ($mapping['consumer_tag'] === $consumerTag) {
                $count++;
            }
        }

        return $count;
    }

    private function consumerCountForChannel(int $channel): int
    {
        $count = 0;
        foreach ($this->activeConsumers as $state) {
            if ($state['channel'] === $channel) {
                $count++;
            }
        }

        return $count;
    }

    private function outboundBodyChunkSize(): int
    {
        return $this->maxFrameSize === 0 ? 131072 : max(1, $this->maxFrameSize);
    }

    private function clearEmptyUnackedChannel(int $channel): void
    {
        if (($this->unackedDeliveries[$channel] ?? []) === []) {
            unset($this->unackedDeliveries[$channel]);
        }
    }

    private function forgetUnackedDelivery(int $channel, int $deliveryTag): void
    {
        unset($this->unackedDeliveries[$channel][$deliveryTag]);
        $this->clearEmptyUnackedChannel($channel);
    }

    private function broker(): Broker
    {
        return $this->broker ?? throw new RuntimeException('AMQP topology operations require a broker.');
    }

    private function authenticator(): AuthenticationService
    {
        return $this->authenticator ?? throw new RuntimeException('AMQP authentication requires an authenticator.');
    }

    private function authorizer(): AuthorizationService
    {
        return $this->authorizer ?? throw new RuntimeException('AMQP authorization requires an authorizer.');
    }

    private function authenticatedUser(): AuthenticatedUser
    {
        return $this->authenticatedUser ?? throw new RuntimeException('AMQP authorization requires an authenticated user.');
    }

    private function authorizeResource(
        int $channel,
        AuthorizationPermission $permission,
        string $resource,
        int $classId,
        int $methodId
    ): bool {
        $result = $this->authorizer()->authorize(
            $this->authenticatedUser(),
            $this->openedVirtualHost(),
            $permission,
            $resource
        );

        if ($result->allowed) {
            return true;
        }

        $this->sendChannelError(
            $channel,
            403,
            sprintf('ACCESS_REFUSED - %s permission refused for resource "%s"', $permission->value, $resource),
            $classId,
            $methodId
        );

        return false;
    }

    private function sendDrainChannelError(int $channel, int $classId, int $methodId): void
    {
        $this->sendChannelError($channel, 320, 'CONNECTION_FORCED - server is draining', $classId, $methodId);
    }

    private function openedVirtualHost(): string
    {
        return $this->virtualHost ?? throw new ProtocolException('AMQP connection has not opened a virtual host.');
    }

    private function sendChannelError(int $channel, int $replyCode, string $replyText, int $classId, int $methodId): void
    {
        $this->closeChannel($channel);
        $this->writeFrame(Frame::methodFrame(
            $channel,
            20,
            40,
            pack('n', $replyCode) . $this->shortString($this->truncateReplyText($replyText)) . pack('nn', $classId, $methodId)
        ));
    }

    private function sendConnectionClose(int $replyCode, string $replyText, int $classId, int $methodId): void
    {
        $this->writeFrame(Frame::methodFrame(
            0,
            10,
            50,
            pack('n', $replyCode) . $this->shortString($this->truncateReplyText($replyText)) . pack('nn', $classId, $methodId)
        ));
        fflush($this->socket);
        $this->close();
    }

    private function negotiateHeartbeat(Frame $frame): void
    {
        if (strlen($frame->payload) < 12) {
            throw new ProtocolException('AMQP tune-ok payload is incomplete.');
        }

        $values = unpack('nchannelMax/NframeMax/nheartbeat', substr($frame->payload, 4, 8));
        $clientHeartbeat = (int) $values['heartbeat'];

        if ($this->heartbeatInterval === 0 || $clientHeartbeat === 0) {
            $this->negotiatedHeartbeat = 0;
            return;
        }

        $this->negotiatedHeartbeat = min($this->heartbeatInterval, $clientHeartbeat);
    }

    private function isHeartbeatTimedOut(): bool
    {
        if ($this->negotiatedHeartbeat === 0 || $this->state !== AmqpConnectionState::Open) {
            return false;
        }

        return $this->elapsedSecondsSince($this->lastReceivedAt) >= ($this->negotiatedHeartbeat * 2);
    }

    private function sendHeartbeatIfIdle(): void
    {
        if ($this->negotiatedHeartbeat === 0 || $this->state !== AmqpConnectionState::Open) {
            return;
        }

        if ($this->elapsedSecondsSince($this->lastSentAt) < $this->negotiatedHeartbeat) {
            return;
        }

        $this->writeFrame(Frame::heartbeatFrame());
    }

    private function elapsedSecondsSince(int $nanos): float
    {
        return ($this->now() - $nanos) / self::NANOS_PER_SECOND;
    }

    private function now(): int
    {
        return ($this->clock)();
    }

    private function replyCodeForTopologyException(TopologyException $exception): int
    {
        return match ($exception->reason) {
            TopologyException::NOT_FOUND => 404,
            TopologyException::ACCESS_REFUSED => 403,
            TopologyException::RESOURCE_LOCKED => 405,
            TopologyException::NOT_IMPLEMENTED => 540,
            default => 406,
        };
    }

    private function truncateReplyText(string $replyText): string
    {
        return strlen($replyText) <= 255 ? $replyText : substr($replyText, 0, 255);
    }

    private function writeFrame(Frame $frame): void
    {
        $bytes = $this->codec->encode($frame);
        $written = 0;

        while ($written < strlen($bytes)) {
            $result = fwrite($this->socket, substr($bytes, $written));
            if ($result === false || $result === 0) {
                throw new ProtocolException('Could not write AMQP frame.');
            }

            $written += $result;
        }

        $this->lastSentAt = $this->now();
    }

    private function connectionStart(): Frame
    {
        return Frame::methodFrame(
            0,
            10,
            10,
            "\x00\x09" . $this->fieldTable([
                'capabilities' => [
                    'basic.nack' => true,
                    'publisher_confirms' => true,
                ],
            ]) . $this->longString('PLAIN') . $this->longString('en_US')
        );
    }

    private function connectionTune(): Frame
    {
        return Frame::methodFrame(0, 10, 30, pack('nNn', 0, $this->maxFrameSize, $this->heartbeatInterval));
    }

    private function connectionOpenOk(): Frame
    {
        return Frame::methodFrame(0, 10, 41, $this->shortString(''));
    }

    /**
     * @param array<string, string> $values
     */
    private function table(array $values): string
    {
        $payload = '';
        foreach ($values as $key => $value) {
            $payload .= $this->shortString($key) . 'S' . $this->longString($value);
        }

        return pack('N', strlen($payload)) . $payload;
    }

    /**
     * @param array<string, mixed> $values
     */
    private function fieldTable(array $values): string
    {
        $payload = '';
        foreach ($values as $key => $value) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            $payload .= $this->shortString($key);
            if (is_bool($value)) {
                $payload .= 't' . chr($value ? 1 : 0);
            } elseif (is_int($value)) {
                $payload .= 'I' . pack('N', $value);
            } elseif (is_array($value)) {
                $payload .= 'F' . $this->fieldTable($value);
            } elseif ($value === null) {
                $payload .= 'V';
            } else {
                $payload .= 'S' . $this->longString((string) $value);
            }
        }

        return pack('N', strlen($payload)) . $payload;
    }

    private function packLongLong(int $value): string
    {
        if ($value < 0) {
            throw new ProtocolException('AMQP long-long cannot be negative.');
        }

        $high = intdiv($value, 4294967296);
        $low = $value % 4294967296;

        return pack('NN', $high, $low);
    }

    private function shortString(string $value): string
    {
        if (strlen($value) > 255) {
            throw new ProtocolException('AMQP short string is too long.');
        }

        return chr(strlen($value)) . $value;
    }

    private function longString(string $value): string
    {
        return pack('N', strlen($value)) . $value;
    }
}
