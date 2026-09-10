<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class Delivery
{
    public function __construct(
        public int $id,
        public int $messageRouteId,
        public int $subscriptionId,
        public int $destinationId,
        public DeliveryState $state,
        public ?string $consumerId,
        public ?string $deliveryTag,
        public int $attempts,
        public ?DateTimeImmutable $reservedAt,
        public ?DateTimeImmutable $acknowledgedAt,
        public DateTimeImmutable $availableAt,
        public DateTimeImmutable $createdAt,
        public DateTimeImmutable $updatedAt
    ) {
    }
}
