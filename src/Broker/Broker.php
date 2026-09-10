<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use Closure;
use FluxQ\Persistence\Postgres\PublishTransaction;
use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\RoutingSourceRepository;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use FluxQ\Runtime\ExclusiveQueueRegistry;
use FluxQ\Support\Uuid;
use PDOException;
use RuntimeException;

final readonly class Broker
{
    private const SERVER_NAMED_QUEUE_MAX_ATTEMPTS = 16;

    private Closure $serverNamedQueueNames;

    public function __construct(
        private VirtualHostRepository $virtualHosts,
        private PublishTransaction $publisher,
        private DestinationRepository $destinations,
        private SubscriptionRepository $subscriptions,
        private DeliveryRepository $deliveries,
        private ?BindingRepository $bindings = null,
        private ?RoutingSourceRepository $routingSources = null,
        private ?MessageRouteRepository $messageRoutes = null,
        private ?MessageRepository $messages = null,
        private ?ResourceLimits $limits = null,
        private ExclusiveQueueRegistry $exclusiveQueues = new ExclusiveQueueRegistry(),
        ?callable $serverNamedQueueNames = null
    ) {
        $this->serverNamedQueueNames = Closure::fromCallable(
            $serverNamedQueueNames ?? static fn (): string => 'amq.gen-' . str_replace('-', '', Uuid::v4())
        );
    }

    public function publish(PublishRequest $request): PublishResult
    {
        $virtualHost = $this->virtualHosts->findByName($request->virtualHost);

        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($request->virtualHost);
        }

        $routingSource = $this->routingSources?->findByName($virtualHost->id, $request->source);
        if ($this->routingSources !== null && $routingSource === null) {
            throw new TopologyException(
                sprintf('Routing source "%s" does not exist.', $request->source),
                TopologyException::NOT_FOUND
            );
        }

        return $this->publisher->publish(
            $virtualHost->id,
            $request->source,
            $request->routingKey,
            $request->payload,
            $request->headers,
            $request->contentType,
            $request->contentEncoding,
            $request->priority,
            $request->persistent,
            $request->messageId,
            $request->metadata,
            $request->persistUnrouted,
            $routingSource?->type ?? RoutingSourceType::Direct
        );
    }

    public function reserve(ReserveRequest $request): ?Delivery
    {
        $virtualHost = $this->virtualHosts->findByName($request->virtualHost);

        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($request->virtualHost);
        }

        $destination = $this->destinations->findByName($virtualHost->id, $request->destination);

        if ($destination === null) {
            throw DestinationNotFoundException::forName($request->virtualHost, $request->destination);
        }
        $this->assertQueueAccess($request->virtualHost, $destination, $request->connectionId);

        $subscription = $this->subscriptions->findByName($destination->id, $request->subscription);

        if ($subscription === null) {
            throw SubscriptionNotFoundException::forName($request->destination, $request->subscription);
        }

        return $this->deliveries->reserveNext($subscription->id, $request->consumerId, $request->deliveryTag);
    }

    public function acknowledge(AcknowledgeRequest $request): Delivery
    {
        return $this->deliveries->acknowledge($request->deliveryId);
    }

    public function reject(RejectRequest $request): Delivery
    {
        $delivery = $this->deliveries->findById($request->deliveryId);
        if ($delivery === null) {
            return $this->deliveries->reject($request->deliveryId);
        }

        $destination = $this->destinations->findById($delivery->destinationId);
        if ($destination === null) {
            return $this->deliveries->reject($request->deliveryId);
        }

        $policy = RetryPolicy::fromDestinationMetadata($destination->metadata);
        if ($policy === null) {
            return $this->deliveries->reject($request->deliveryId);
        }

        $deadLetterDestinationId = null;
        if ($policy->deadLetterDestination !== null) {
            $deadLetterDestination = $this->destinations->findByName($destination->virtualHostId, $policy->deadLetterDestination);
            if ($deadLetterDestination === null || $deadLetterDestination->type !== DestinationType::Queue) {
                throw new TopologyException(
                    sprintf('Dead-letter destination "%s" does not exist.', $policy->deadLetterDestination),
                    TopologyException::NOT_FOUND
                );
            }

            $deadLetterDestinationId = $deadLetterDestination->id;
        }

        return $this->deliveries->fail($request->deliveryId, $policy, $deadLetterDestinationId);
    }

    public function release(ReleaseRequest $request): Delivery
    {
        return $this->deliveries->release($request->deliveryId, $request->availableAt);
    }

    /**
     * @param array<string, mixed> $headers
     */
    public function publishToDefaultExchange(
        string $virtualHostName,
        string $queue,
        string $payload,
        array $headers = [],
        ?string $contentType = null,
        ?string $contentEncoding = null,
        int $priority = 0,
        bool $persistent = true,
        ?string $messageId = null,
        array $metadata = [],
        ?string $connectionId = null
    ): PublishResult {
        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $destination = $this->destinations->findByName($virtualHost->id, $queue);
        if ($destination === null || $destination->type !== DestinationType::Queue) {
            throw new TopologyException(sprintf('Queue "%s" does not exist.', $queue), TopologyException::NOT_FOUND);
        }
        $this->assertQueueAccess($virtualHostName, $destination, $connectionId);

        return $this->publisher->publishToDestination(
            $destination->id,
            $payload,
            $headers,
            $contentType,
            $contentEncoding,
            $priority,
            $persistent,
            $messageId,
            $metadata
        );
    }

    public function ensureQueueSubscription(
        string $virtualHostName,
        string $queue,
        string $subscriptionName,
        ?string $connectionId = null
    ): Subscription
    {
        if ($subscriptionName === '') {
            throw new TopologyException('Subscription name must not be empty.', TopologyException::PRECONDITION_FAILED);
        }

        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $destination = $this->destinations->findByName($virtualHost->id, $queue);
        if ($destination === null || $destination->type !== DestinationType::Queue) {
            throw new TopologyException(sprintf('Queue "%s" does not exist.', $queue), TopologyException::NOT_FOUND);
        }
        $this->assertQueueAccess($virtualHostName, $destination, $connectionId);

        $subscription = $this->subscriptions->findByName($destination->id, $subscriptionName);
        if ($subscription !== null) {
            return $subscription;
        }

        return $this->subscriptions->create($destination->id, $subscriptionName, false, ['declared_by' => 'amqp']);
    }

    public function messageForDelivery(Delivery $delivery): Message
    {
        $route = $this->messageRoutes()->findById($delivery->messageRouteId)
            ?? throw new RuntimeException(sprintf('Message route %d does not exist.', $delivery->messageRouteId));

        return $this->messages()->findById($route->messageId)
            ?? throw new RuntimeException(sprintf('Message %d does not exist.', $route->messageId));
    }

    public function readyMessageCount(Destination $destination): int
    {
        return $this->deliveries->countReadyByDestination($destination->id);
    }

    public function queueStatus(string $virtualHostName, string $name, ?string $connectionId = null): QueueStatus
    {
        if ($name === '') {
            throw new TopologyException('Queue name must not be empty.', TopologyException::PRECONDITION_FAILED);
        }

        $destination = $this->queueDestination($virtualHostName, $name, $connectionId);

        return new QueueStatus($destination, $this->readyMessageCount($destination));
    }

    public function declareQueue(
        string $virtualHostName,
        string $name,
        bool $durable,
        bool $autoDelete,
        bool $passive = false,
        bool $exclusive = false,
        ?string $connectionId = null
    ): Destination {
        if ($name === '' && $passive) {
            throw new TopologyException('Queue name must not be empty.', TopologyException::PRECONDITION_FAILED);
        }

        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        if ($name === '') {
            return $this->declareServerNamedQueue(
                $virtualHostName,
                $virtualHost->id,
                $durable,
                $autoDelete,
                $exclusive,
                $connectionId
            );
        }

        $destination = $this->destinations->findByName($virtualHost->id, $name);
        if ($destination === null) {
            if ($passive) {
                throw new TopologyException(sprintf('Queue "%s" does not exist.', $name), TopologyException::NOT_FOUND);
            }

            $limits = $this->limits ?? new ResourceLimits();
            if (!$limits->allows($limits->maxQueuesPerVirtualHost, $this->destinations->countQueuesByVirtualHost($virtualHost->id))) {
                throw new ResourceLimitException(sprintf('Queue limit reached for virtual host "%s".', $virtualHostName));
            }

            $destination = $this->destinations->create(
                $virtualHost->id,
                $name,
                DestinationType::Queue,
                $durable,
                $autoDelete,
                ['declared_by' => 'topology', 'exclusive' => $exclusive]
            );
            if ($exclusive) {
                $this->exclusiveQueues()->claim($virtualHostName, $destination->name, $this->requiredConnectionId($connectionId));
            }

            $this->ensureQueueSubscription($virtualHostName, $destination->name, 'amqp', $connectionId);

            return $destination;
        }

        if ($passive) {
            if ($destination->type !== DestinationType::Queue) {
                throw new TopologyException(sprintf('Queue "%s" does not exist.', $name), TopologyException::NOT_FOUND);
            }
            $this->assertQueueAccess($virtualHostName, $destination, $connectionId);

            return $destination;
        }

        $this->assertQueueAccess($virtualHostName, $destination, $connectionId);

        if (
            $destination->type !== DestinationType::Queue
            || $destination->durable !== $durable
            || $destination->autoDelete !== $autoDelete
            || $destination->exclusive() !== $exclusive
        ) {
            throw new TopologyException(
                sprintf('Queue "%s" exists with incompatible properties.', $name),
                TopologyException::PRECONDITION_FAILED
            );
        }

        $this->ensureQueueSubscription($virtualHostName, $destination->name, 'amqp', $connectionId);

        return $destination;
    }

    private function declareServerNamedQueue(
        string $virtualHostName,
        int $virtualHostId,
        bool $durable,
        bool $autoDelete,
        bool $exclusive,
        ?string $connectionId
    ): Destination {
        $limits = $this->limits ?? new ResourceLimits();
        if (!$limits->allows($limits->maxQueuesPerVirtualHost, $this->destinations->countQueuesByVirtualHost($virtualHostId))) {
            throw new ResourceLimitException(sprintf('Queue limit reached for virtual host "%s".', $virtualHostName));
        }

        for ($attempt = 0; $attempt < self::SERVER_NAMED_QUEUE_MAX_ATTEMPTS; $attempt++) {
            $name = ($this->serverNamedQueueNames)();
            if ($name === '') {
                continue;
            }

            if ($this->destinations->findByName($virtualHostId, $name) !== null) {
                continue;
            }

            try {
                $destination = $this->destinations->create(
                    $virtualHostId,
                    $name,
                    DestinationType::Queue,
                    $durable,
                    $autoDelete,
                    ['declared_by' => 'topology', 'exclusive' => $exclusive]
                );
            } catch (PDOException $exception) {
                if ($exception->getCode() === '23505') {
                    continue;
                }

                throw $exception;
            }
            if ($exclusive) {
                $this->exclusiveQueues()->claim($virtualHostName, $destination->name, $this->requiredConnectionId($connectionId));
            }

            $this->ensureQueueSubscription($virtualHostName, $destination->name, 'amqp', $connectionId);

            return $destination;
        }

        throw new RuntimeException('Could not generate a unique server-named queue.');
    }

    public function declareDirectRoutingSource(
        string $virtualHostName,
        string $name,
        bool $durable,
        bool $autoDelete,
        bool $passive = false
    ): RoutingSource {
        return $this->declareRoutingSource(
            $virtualHostName,
            $name,
            RoutingSourceType::Direct,
            $durable,
            $autoDelete,
            $passive
        );
    }

    public function declareFanoutRoutingSource(
        string $virtualHostName,
        string $name,
        bool $durable,
        bool $autoDelete,
        bool $passive = false
    ): RoutingSource {
        return $this->declareRoutingSource(
            $virtualHostName,
            $name,
            RoutingSourceType::Fanout,
            $durable,
            $autoDelete,
            $passive
        );
    }

    public function declareTopicRoutingSource(
        string $virtualHostName,
        string $name,
        bool $durable,
        bool $autoDelete,
        bool $passive = false
    ): RoutingSource {
        return $this->declareRoutingSource(
            $virtualHostName,
            $name,
            RoutingSourceType::Topic,
            $durable,
            $autoDelete,
            $passive
        );
    }

    public function routingSourceStatus(
        string $virtualHostName,
        string $name,
        RoutingSourceType $type
    ): RoutingSource {
        if ($name === '') {
            throw new TopologyException(
                'The default AMQP exchange is implicit and cannot be declared.',
                TopologyException::PRECONDITION_FAILED
            );
        }

        $routingSources = $this->routingSources();
        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $source = $routingSources->findByName($virtualHost->id, $name);
        if ($source === null) {
            throw new TopologyException(
                sprintf('Routing source "%s" does not exist.', $name),
                TopologyException::NOT_FOUND
            );
        }

        if ($source->type !== $type) {
            throw new TopologyException(
                sprintf('Routing source "%s" exists with incompatible properties.', $name),
                TopologyException::PRECONDITION_FAILED
            );
        }

        return $source;
    }

    private function declareRoutingSource(
        string $virtualHostName,
        string $name,
        RoutingSourceType $type,
        bool $durable,
        bool $autoDelete,
        bool $passive = false
    ): RoutingSource {
        if ($name === '') {
            throw new TopologyException(
                'The default AMQP exchange is implicit and cannot be declared.',
                TopologyException::PRECONDITION_FAILED
            );
        }

        if ($passive) {
            return $this->routingSourceStatus($virtualHostName, $name, $type);
        }

        $routingSources = $this->routingSources();
        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $source = $routingSources->findByName($virtualHost->id, $name);
        if ($source === null) {
            return $routingSources->create(
                $virtualHost->id,
                $name,
                $type,
                $durable,
                $autoDelete,
                ['declared_by' => 'topology']
            );
        }

        if (
            $source->type !== $type
            || $source->durable !== $durable
            || $source->autoDelete !== $autoDelete
        ) {
            throw new TopologyException(
                sprintf('Routing source "%s" exists with incompatible properties.', $name),
                TopologyException::PRECONDITION_FAILED
            );
        }

        return $source;
    }

    public function bindQueue(
        string $virtualHostName,
        string $source,
        string $queue,
        string $routingKey,
        ?string $connectionId = null
    ): ?Binding
    {
        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $destination = $this->destinations->findByName($virtualHost->id, $queue);
        if ($destination === null || $destination->type !== DestinationType::Queue) {
            throw new TopologyException(sprintf('Queue "%s" does not exist.', $queue), TopologyException::NOT_FOUND);
        }
        $this->assertQueueAccess($virtualHostName, $destination, $connectionId);

        if ($source === '') {
            if ($routingKey !== $queue) {
                throw new TopologyException(
                    'The default routing source only binds a queue to its own name.',
                    TopologyException::PRECONDITION_FAILED
                );
            }

            return null;
        }

        $bindings = $this->bindings();
        $routingSources = $this->routingSources();
        if ($routingSources->findByName($virtualHost->id, $source) === null) {
            throw new TopologyException(sprintf('Routing source "%s" does not exist.', $source), TopologyException::NOT_FOUND);
        }

        $binding = $bindings->findExact($virtualHost->id, $source, $destination->id, $routingKey);
        if ($binding !== null) {
            return $binding;
        }

        return $bindings->create($virtualHost->id, $source, $destination->id, $routingKey, ['declared_by' => 'topology']);
    }

    public function purgeQueue(string $virtualHostName, string $queue, ?string $connectionId = null): int
    {
        $destination = $this->queueDestination($virtualHostName, $queue, $connectionId);

        return $this->deliveries->rejectOutstandingByDestination($destination->id);
    }

    public function deleteQueue(string $virtualHostName, string $queue, bool $ifEmpty = false, ?string $connectionId = null): int
    {
        $this->assertQueueDeletable($virtualHostName, $queue, $ifEmpty, $connectionId);
        $destination = $this->queueDestination($virtualHostName, $queue, $connectionId);

        $count = $this->destinations->deleteQueueGraph($destination->id);
        $this->exclusiveQueues()->release($virtualHostName, $destination->name);

        return $count;
    }

    public function assertQueueDeletable(string $virtualHostName, string $queue, bool $ifEmpty = false, ?string $connectionId = null): void
    {
        $destination = $this->queueDestination($virtualHostName, $queue, $connectionId);

        if ($ifEmpty && $this->deliveries->countOutstandingByDestination($destination->id) > 0) {
            throw new TopologyException(sprintf('Queue "%s" is not empty.', $queue), TopologyException::PRECONDITION_FAILED);
        }
    }

    public function unbindQueue(
        string $virtualHostName,
        string $source,
        string $queue,
        string $routingKey,
        ?string $connectionId = null
    ): void
    {
        if ($source === '') {
            throw new TopologyException(
                'The default AMQP exchange binding is implicit and cannot be unbound.',
                TopologyException::PRECONDITION_FAILED
            );
        }

        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $destination = $this->destinations->findByName($virtualHost->id, $queue);
        if ($destination === null || $destination->type !== DestinationType::Queue) {
            throw new TopologyException(sprintf('Queue "%s" does not exist.', $queue), TopologyException::NOT_FOUND);
        }
        $this->assertQueueAccess($virtualHostName, $destination, $connectionId);

        if ($this->routingSources()->findByName($virtualHost->id, $source) === null) {
            throw new TopologyException(sprintf('Routing source "%s" does not exist.', $source), TopologyException::NOT_FOUND);
        }

        $this->bindings()->deleteExact($virtualHost->id, $source, $destination->id, $routingKey);
    }

    public function deleteRoutingSource(string $virtualHostName, string $name, bool $ifUnused = false): void
    {
        if ($name === '') {
            throw new TopologyException(
                'The default AMQP exchange is implicit and cannot be deleted.',
                TopologyException::PRECONDITION_FAILED
            );
        }

        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $routingSources = $this->routingSources();
        if ($routingSources->findByName($virtualHost->id, $name) === null) {
            throw new TopologyException(sprintf('Routing source "%s" does not exist.', $name), TopologyException::NOT_FOUND);
        }

        $bindings = $this->bindings();
        if ($ifUnused && $bindings->countBySource($virtualHost->id, $name) > 0) {
            throw new TopologyException(sprintf('Routing source "%s" is in use.', $name), TopologyException::PRECONDITION_FAILED);
        }

        $routingSources->deleteGraph($virtualHost->id, $name);
    }

    public function deleteExclusiveQueuesForConnection(string $connectionId): int
    {
        $deleted = 0;

        foreach ($this->exclusiveQueues()->queuesOwnedBy($connectionId) as $owned) {
            try {
                $this->deleteQueue($owned['virtual_host'], $owned['queue'], connectionId: $connectionId);
                $deleted++;
            } catch (TopologyException $exception) {
                if ($exception->reason !== TopologyException::NOT_FOUND) {
                    throw $exception;
                }
            } finally {
                $this->exclusiveQueues()->release($owned['virtual_host'], $owned['queue']);
            }
        }

        $this->exclusiveQueues()->releaseByConnection($connectionId);

        return $deleted;
    }

    public function deletePersistedExclusiveQueues(): int
    {
        $deleted = 0;

        foreach ($this->destinations->allExclusiveQueues() as $destination) {
            $this->destinations->deleteQueueGraph($destination->id);
            $deleted++;
        }

        $this->exclusiveQueues()->clear();

        return $deleted;
    }

    private function queueDestination(string $virtualHostName, string $queue, ?string $connectionId = null): Destination
    {
        $virtualHost = $this->virtualHosts->findByName($virtualHostName);
        if ($virtualHost === null) {
            throw VirtualHostNotFoundException::forName($virtualHostName);
        }

        $destination = $this->destinations->findByName($virtualHost->id, $queue);
        if ($destination === null || $destination->type !== DestinationType::Queue) {
            throw new TopologyException(sprintf('Queue "%s" does not exist.', $queue), TopologyException::NOT_FOUND);
        }
        $this->assertQueueAccess($virtualHostName, $destination, $connectionId);

        return $destination;
    }

    private function assertQueueAccess(string $virtualHostName, Destination $destination, ?string $connectionId): void
    {
        if (!$destination->exclusive()) {
            return;
        }

        if ($this->exclusiveQueues()->isOwner($virtualHostName, $destination->name, $connectionId)) {
            return;
        }

        throw new TopologyException(
            sprintf('Queue "%s" is exclusive to another connection.', $destination->name),
            TopologyException::RESOURCE_LOCKED
        );
    }

    private function requiredConnectionId(?string $connectionId): string
    {
        if ($connectionId === null || $connectionId === '') {
            throw new TopologyException(
                'Exclusive queues require a runtime connection owner.',
                TopologyException::PRECONDITION_FAILED
            );
        }

        return $connectionId;
    }

    private function bindings(): BindingRepository
    {
        return $this->bindings ?? throw new RuntimeException('Broker topology binding repository is not configured.');
    }

    private function routingSources(): RoutingSourceRepository
    {
        return $this->routingSources ?? throw new RuntimeException('Broker topology routing source repository is not configured.');
    }

    private function messageRoutes(): MessageRouteRepository
    {
        return $this->messageRoutes ?? throw new RuntimeException('Broker message route repository is not configured.');
    }

    private function messages(): MessageRepository
    {
        return $this->messages ?? throw new RuntimeException('Broker message repository is not configured.');
    }

    private function exclusiveQueues(): ExclusiveQueueRegistry
    {
        return $this->exclusiveQueues;
    }
}
