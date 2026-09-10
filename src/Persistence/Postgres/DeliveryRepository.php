<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\Delivery;
use FluxQ\Broker\DeliveryState;
use FluxQ\Broker\RetryPolicy;
use PDO;
use RuntimeException;

final readonly class DeliveryRepository
{
    private const COLUMNS = <<<SQL
id, message_route_id, subscription_id, destination_id, state, consumer_id, delivery_tag, attempts,
reserved_at, acknowledged_at, available_at, created_at, updated_at
SQL;

    public function __construct(
        private Connection $connection
    ) {
    }

    public function create(
        int $messageRouteId,
        int $subscriptionId,
        ?DateTimeImmutable $availableAt = null
    ): Delivery {
        if ($availableAt === null) {
            $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
INSERT INTO deliveries (message_route_id, subscription_id, destination_id)
VALUES (:message_route_id, :subscription_id, (
    SELECT destination_id FROM message_routes WHERE id = :message_route_id
))
RETURNING %s
SQL, self::COLUMNS));
        } else {
            $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
INSERT INTO deliveries (message_route_id, subscription_id, destination_id, available_at)
VALUES (:message_route_id, :subscription_id, (
    SELECT destination_id FROM message_routes WHERE id = :message_route_id
), :available_at)
RETURNING %s
SQL, self::COLUMNS));
            $statement->bindValue('available_at', $this->formatTimestamp($availableAt));
        }

        $statement->bindValue('message_route_id', $messageRouteId, PDO::PARAM_INT);
        $statement->bindValue('subscription_id', $subscriptionId, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created delivery.');
        }

        return $this->mapRow($row);
    }

    public function findById(int $id): ?Delivery
    {
        $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
SELECT %s
FROM deliveries
WHERE id = :id
SQL, self::COLUMNS));
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    /**
     * @return list<Delivery>
     */
    public function allBySubscription(int $subscriptionId): array
    {
        $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
SELECT %s
FROM deliveries
WHERE subscription_id = :subscription_id
ORDER BY id
SQL, self::COLUMNS));
        $statement->bindValue('subscription_id', $subscriptionId, PDO::PARAM_INT);
        $statement->execute();

        return array_map(
            fn (array $row): Delivery => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    /**
     * @return array<string, int>
     */
    public function countByState(): array
    {
        $statement = $this->connection->pdo()->query(<<<'SQL'
SELECT state, COUNT(*) AS count
FROM deliveries
GROUP BY state
SQL);

        if ($statement === false) {
            throw new RuntimeException('Could not count deliveries in PostgreSQL.');
        }

        $counts = [];
        foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $counts[(string) $row['state']] = (int) $row['count'];
        }

        return $counts;
    }

    /**
     * @return array<string, int>
     */
    public function countByStateForDestination(int $destinationId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT state, COUNT(*) AS count
FROM deliveries
WHERE destination_id = :destination_id
GROUP BY state
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        $counts = [];
        foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $counts[(string) $row['state']] = (int) $row['count'];
        }

        return $counts;
    }

    public function countOutstandingByDestination(int $destinationId): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM deliveries
WHERE destination_id = :destination_id
  AND state IN ('pending', 'reserved')
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    public function countReadyByDestination(int $destinationId): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT COUNT(*)
FROM deliveries
WHERE destination_id = :destination_id
  AND state = 'pending'
  AND available_at <= CURRENT_TIMESTAMP
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return (int) $statement->fetchColumn();
    }

    public function rejectOutstandingByDestination(int $destinationId): int
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
UPDATE deliveries
SET state = 'rejected',
    consumer_id = NULL,
    delivery_tag = NULL,
    reserved_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE destination_id = :destination_id
  AND state IN ('pending', 'reserved')
SQL);
        $statement->bindValue('destination_id', $destinationId, PDO::PARAM_INT);
        $statement->execute();

        return $statement->rowCount();
    }

    public function reserveNext(int $subscriptionId, string $consumerId, ?string $deliveryTag = null): ?Delivery
    {
        return $this->connection->transaction(function (PDO $pdo) use ($subscriptionId, $consumerId, $deliveryTag): ?Delivery {
            $select = $pdo->prepare(<<<'SQL'
SELECT id
FROM deliveries
WHERE subscription_id = :subscription_id
  AND state = 'pending'
  AND available_at <= CURRENT_TIMESTAMP
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 1
SQL);
            $select->bindValue('subscription_id', $subscriptionId, PDO::PARAM_INT);
            $select->execute();

            $id = $select->fetchColumn();

            if ($id === false) {
                return null;
            }

            $update = $pdo->prepare(sprintf(<<<'SQL'
UPDATE deliveries
SET state = 'reserved',
    consumer_id = :consumer_id,
    delivery_tag = :delivery_tag,
    reserved_at = CURRENT_TIMESTAMP,
    attempts = attempts + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :id
  AND state = 'pending'
RETURNING %s
SQL, self::COLUMNS));
            $update->bindValue('id', (int) $id, PDO::PARAM_INT);
            $update->bindValue('consumer_id', $consumerId);
            $update->bindValue('delivery_tag', $deliveryTag);
            $update->execute();

            $row = $update->fetch(PDO::FETCH_ASSOC);

            if (!is_array($row)) {
                throw new RuntimeException(sprintf('Delivery %d could not be reserved after it was selected.', (int) $id));
            }

            return $this->mapRow($row);
        });
    }

    public function acknowledge(int $id): Delivery
    {
        return $this->transition($id, DeliveryState::Reserved, DeliveryState::Acknowledged, <<<SQL
state = 'acknowledged',
acknowledged_at = CURRENT_TIMESTAMP,
updated_at = CURRENT_TIMESTAMP
SQL);
    }

    public function reject(int $id): Delivery
    {
        return $this->transition($id, DeliveryState::Reserved, DeliveryState::Rejected, <<<SQL
state = 'rejected',
updated_at = CURRENT_TIMESTAMP
SQL);
    }

    public function fail(int $id, ?RetryPolicy $policy = null, ?int $deadLetterDestinationId = null): Delivery
    {
        if ($policy === null) {
            return $this->reject($id);
        }

        return $this->connection->transaction(function (PDO $pdo) use ($id, $policy, $deadLetterDestinationId): Delivery {
            $delivery = $this->selectForUpdate($pdo, $id);

            if ($delivery->state !== DeliveryState::Reserved) {
                $this->throwInvalidTransition($id, DeliveryState::Rejected);
            }

            if ($delivery->attempts < $policy->maxAttempts) {
                return $this->releaseWithPdo(
                    $pdo,
                    $id,
                    (new DateTimeImmutable())->modify(sprintf('+%d seconds', $policy->retryDelaySeconds))
                );
            }

            if ($deadLetterDestinationId !== null) {
                $route = $this->deadLetterRoute($pdo, $delivery->messageRouteId, $deadLetterDestinationId);
                $this->createDeadLetterDeliveries($pdo, $route['id'], $deadLetterDestinationId);
            }

            return $this->rejectWithPdo($pdo, $id);
        });
    }

    public function release(int $id, ?DateTimeImmutable $availableAt = null): Delivery
    {
        if ($availableAt === null) {
            $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
UPDATE deliveries
SET state = 'pending',
    consumer_id = NULL,
    delivery_tag = NULL,
    reserved_at = NULL,
    available_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :id
  AND state = 'reserved'
RETURNING %s
SQL, self::COLUMNS));
        } else {
            $statement = $this->connection->pdo()->prepare(sprintf(<<<'SQL'
UPDATE deliveries
SET state = 'pending',
    consumer_id = NULL,
    delivery_tag = NULL,
    reserved_at = NULL,
    available_at = :available_at,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :id
  AND state = 'reserved'
RETURNING %s
SQL, self::COLUMNS));
            $statement->bindValue('available_at', $this->formatTimestamp($availableAt));
        }

        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (is_array($row)) {
            return $this->mapRow($row);
        }

        $this->throwInvalidTransition($id, DeliveryState::Pending);
    }

    private function transition(
        int $id,
        DeliveryState $from,
        DeliveryState $to,
        string $assignments
    ): Delivery {
        $statement = $this->connection->pdo()->prepare(sprintf(<<<SQL
UPDATE deliveries
SET %s
WHERE id = :id
  AND state = :from_state
RETURNING %s
SQL, $assignments, self::COLUMNS));
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->bindValue('from_state', $from->value);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (is_array($row)) {
            return $this->mapRow($row);
        }

        $this->throwInvalidTransition($id, $to);
    }

    private function throwInvalidTransition(int $id, DeliveryState $to): never
    {
        $delivery = $this->findById($id);

        if ($delivery === null) {
            throw DeliveryStateException::notFound($id);
        }

        throw DeliveryStateException::invalidTransition($id, $delivery->state->value, $to->value);
    }

    private function selectForUpdate(PDO $pdo, int $id): Delivery
    {
        $statement = $pdo->prepare(sprintf(<<<'SQL'
SELECT %s
FROM deliveries
WHERE id = :id
FOR UPDATE
SQL, self::COLUMNS));
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw DeliveryStateException::notFound($id);
        }

        return $this->mapRow($row);
    }

    private function releaseWithPdo(PDO $pdo, int $id, DateTimeImmutable $availableAt): Delivery
    {
        $statement = $pdo->prepare(sprintf(<<<'SQL'
UPDATE deliveries
SET state = 'pending',
    consumer_id = NULL,
    delivery_tag = NULL,
    reserved_at = NULL,
    available_at = :available_at,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :id
  AND state = 'reserved'
RETURNING %s
SQL, self::COLUMNS));
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->bindValue('available_at', $this->formatTimestamp($availableAt));
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            $this->throwInvalidTransition($id, DeliveryState::Pending);
        }

        return $this->mapRow($row);
    }

    private function rejectWithPdo(PDO $pdo, int $id): Delivery
    {
        $statement = $pdo->prepare(sprintf(<<<'SQL'
UPDATE deliveries
SET state = 'rejected',
    updated_at = CURRENT_TIMESTAMP
WHERE id = :id
  AND state = 'reserved'
RETURNING %s
SQL, self::COLUMNS));
        $statement->bindValue('id', $id, PDO::PARAM_INT);
        $statement->execute();

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            $this->throwInvalidTransition($id, DeliveryState::Rejected);
        }

        return $this->mapRow($row);
    }

    /**
     * @return array{id: int, message_id: int}
     */
    private function deadLetterRoute(PDO $pdo, int $messageRouteId, int $deadLetterDestinationId): array
    {
        $source = $pdo->prepare(<<<'SQL'
SELECT message_id
FROM message_routes
WHERE id = :id
SQL);
        $source->bindValue('id', $messageRouteId, PDO::PARAM_INT);
        $source->execute();

        $messageId = $source->fetchColumn();
        if ($messageId === false) {
            throw new RuntimeException(sprintf('Message route %d does not exist.', $messageRouteId));
        }

        $insert = $pdo->prepare(<<<'SQL'
INSERT INTO message_routes (message_id, destination_id)
VALUES (:message_id, :destination_id)
ON CONFLICT (message_id, destination_id) DO UPDATE
SET destination_id = EXCLUDED.destination_id
RETURNING id, message_id
SQL);
        $insert->bindValue('message_id', (int) $messageId, PDO::PARAM_INT);
        $insert->bindValue('destination_id', $deadLetterDestinationId, PDO::PARAM_INT);
        $insert->execute();

        $row = $insert->fetch(PDO::FETCH_ASSOC);
        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the dead-letter message route.');
        }

        return ['id' => (int) $row['id'], 'message_id' => (int) $row['message_id']];
    }

    private function createDeadLetterDeliveries(PDO $pdo, int $messageRouteId, int $deadLetterDestinationId): void
    {
        $statement = $pdo->prepare(<<<'SQL'
INSERT INTO deliveries (message_route_id, subscription_id, destination_id)
SELECT :message_route_id, subscriptions.id, :destination_id
FROM subscriptions
WHERE subscriptions.destination_id = :destination_id
ON CONFLICT (message_route_id, subscription_id) DO NOTHING
SQL);
        $statement->bindValue('message_route_id', $messageRouteId, PDO::PARAM_INT);
        $statement->bindValue('destination_id', $deadLetterDestinationId, PDO::PARAM_INT);
        $statement->execute();
    }

    /**
     * @param array{
     *     id: mixed,
     *     message_route_id: mixed,
     *     subscription_id: mixed,
     *     destination_id: mixed,
     *     state: mixed,
     *     consumer_id: mixed,
     *     delivery_tag: mixed,
     *     attempts: mixed,
     *     reserved_at: mixed,
     *     acknowledged_at: mixed,
     *     available_at: mixed,
     *     created_at: mixed,
     *     updated_at: mixed
     * } $row
     */
    private function mapRow(array $row): Delivery
    {
        return new Delivery(
            (int) $row['id'],
            (int) $row['message_route_id'],
            (int) $row['subscription_id'],
            (int) $row['destination_id'],
            DeliveryState::from((string) $row['state']),
            $row['consumer_id'] === null ? null : (string) $row['consumer_id'],
            $row['delivery_tag'] === null ? null : (string) $row['delivery_tag'],
            (int) $row['attempts'],
            $row['reserved_at'] === null ? null : new DateTimeImmutable((string) $row['reserved_at']),
            $row['acknowledged_at'] === null ? null : new DateTimeImmutable((string) $row['acknowledged_at']),
            new DateTimeImmutable((string) $row['available_at']),
            new DateTimeImmutable((string) $row['created_at']),
            new DateTimeImmutable((string) $row['updated_at'])
        );
    }

    private function formatTimestamp(DateTimeImmutable $timestamp): string
    {
        return $timestamp->format('Y-m-d H:i:s.uP');
    }
}
