<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use FluxQ\Broker\Binding;
use FluxQ\Broker\PublishResult;
use FluxQ\Broker\ResourceLimitException;
use FluxQ\Broker\ResourceLimits;
use FluxQ\Broker\RoutingSourceType;
use FluxQ\Broker\TopicMatcher;
use PDO;

final readonly class PublishTransaction
{
    private MessageRepository $messages;
    private BindingRepository $bindings;
    private TopicMatcher $topics;
    private MessageRouteRepository $routes;
    private SubscriptionRepository $subscriptions;
    private DeliveryRepository $deliveries;

    public function __construct(
        private Connection $connection,
        private ResourceLimits $limits = new ResourceLimits()
    )
    {
        $this->messages = new MessageRepository($connection);
        $this->bindings = new BindingRepository($connection);
        $this->topics = new TopicMatcher();
        $this->routes = new MessageRouteRepository($connection);
        $this->subscriptions = new SubscriptionRepository($connection);
        $this->deliveries = new DeliveryRepository($connection);
    }

    /**
     * @param array<string, mixed> $headers
     * @param array<string, mixed> $metadata
     */
    public function publish(
        int $virtualHostId,
        string $source,
        string $routingKey,
        string $payload,
        array $headers = [],
        ?string $contentType = null,
        ?string $contentEncoding = null,
        int $priority = 0,
        bool $persistent = true,
        ?string $messageId = null,
        array $metadata = [],
        bool $persistUnrouted = true,
        RoutingSourceType $sourceType = RoutingSourceType::Direct
    ): PublishResult {
        return $this->connection->transaction(function () use (
            $virtualHostId,
            $source,
            $routingKey,
            $payload,
            $headers,
            $contentType,
            $contentEncoding,
            $priority,
            $persistent,
            $messageId,
            $metadata,
            $persistUnrouted,
            $sourceType
        ): PublishResult {
            $bindings = match ($sourceType) {
                RoutingSourceType::Direct => $this->bindings->findForRoute($virtualHostId, $source, $routingKey),
                RoutingSourceType::Fanout => $this->bindings->findForSource($virtualHostId, $source),
                RoutingSourceType::Topic => $this->topicBindings($virtualHostId, $source, $routingKey),
            };
            $destinationIds = $this->uniqueDestinationIds($bindings);
            if ($destinationIds === [] && !$persistUnrouted) {
                return new PublishResult(null, [], []);
            }

            $this->assertQueueDepthCapacity($destinationIds);

            $message = $this->messages->create(
                $payload,
                $headers,
                $contentType,
                $contentEncoding,
                $priority,
                $persistent,
                $messageId,
                $metadata
            );

            $routes = [];
            $deliveries = [];

            foreach ($destinationIds as $destinationId) {
                $route = $this->routes->create($message->id, $destinationId);
                $routes[] = $route;

                foreach ($this->subscriptions->allByDestination($destinationId) as $subscription) {
                    $deliveries[] = $this->deliveries->create($route->id, $subscription->id);
                }
            }

            return new PublishResult($message, $routes, $deliveries);
        });
    }

    /**
     * @param array<string, mixed> $headers
     * @param array<string, mixed> $metadata
     */
    public function publishToDestination(
        int $destinationId,
        string $payload,
        array $headers = [],
        ?string $contentType = null,
        ?string $contentEncoding = null,
        int $priority = 0,
        bool $persistent = true,
        ?string $messageId = null,
        array $metadata = []
    ): PublishResult {
        return $this->connection->transaction(function () use (
            $destinationId,
            $payload,
            $headers,
            $contentType,
            $contentEncoding,
            $priority,
            $persistent,
            $messageId,
            $metadata
        ): PublishResult {
            $this->assertQueueDepthCapacity([$destinationId]);

            $message = $this->messages->create(
                $payload,
                $headers,
                $contentType,
                $contentEncoding,
                $priority,
                $persistent,
                $messageId,
                $metadata
            );

            $route = $this->routes->create($message->id, $destinationId);
            $deliveries = [];

            foreach ($this->subscriptions->allByDestination($destinationId) as $subscription) {
                $deliveries[] = $this->deliveries->create($route->id, $subscription->id);
            }

            return new PublishResult($message, [$route], $deliveries);
        });
    }

    /**
     * @return list<Binding>
     */
    private function topicBindings(int $virtualHostId, string $source, string $routingKey): array
    {
        return array_values(array_filter(
            $this->bindings->findForSource($virtualHostId, $source),
            fn (Binding $binding): bool => $this->topics->matches($binding->routingKey, $routingKey)
        ));
    }

    /**
     * @param list<Binding> $bindings
     * @return list<int>
     */
    private function uniqueDestinationIds(array $bindings): array
    {
        $seen = [];
        $destinationIds = [];

        foreach ($bindings as $binding) {
            if (isset($seen[$binding->destinationId])) {
                continue;
            }

            $seen[$binding->destinationId] = true;
            $destinationIds[] = $binding->destinationId;
        }

        return $destinationIds;
    }

    /**
     * @param list<int> $destinationIds
     */
    private function assertQueueDepthCapacity(array $destinationIds): void
    {
        if ($this->limits->maxQueueDepth === 0 || $destinationIds === []) {
            return;
        }

        $pdo = $this->connection->pdo();
        foreach ($destinationIds as $destinationId) {
            $this->lockDestination($pdo, $destinationId);
            $currentDepth = $this->outstandingDepth($pdo, $destinationId);
            $newDeliveries = $this->subscriptionCount($pdo, $destinationId);

            if (!$this->limits->allows($this->limits->maxQueueDepth, $currentDepth, $newDeliveries)) {
                throw new ResourceLimitException(sprintf('Queue depth limit reached for destination %d.', $destinationId));
            }
        }
    }

    private function lockDestination(PDO $pdo, int $destinationId): void
    {
        $statement = $pdo->prepare('SELECT id FROM destinations WHERE id = :id FOR UPDATE');
        $statement->bindValue('id', $destinationId, PDO::PARAM_INT);
        $statement->execute();
    }

    private function outstandingDepth(PDO $pdo, int $destinationId): int
    {
        $statement = $pdo->prepare(<<<'SQL'
SELECT COUNT(*)
FROM deliveries
WHERE destination_id = :destination_id
  AND state IN ('pending', 'reserved')
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    private function subscriptionCount(PDO $pdo, int $destinationId): int
    {
        $statement = $pdo->prepare('SELECT COUNT(*) FROM subscriptions WHERE destination_id = :destination_id');
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }
}
