<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;
use InvalidArgumentException;

final readonly class ReleaseRequest
{
    public function __construct(
        public int $deliveryId,
        public ?DateTimeImmutable $availableAt = null
    ) {
        if ($this->deliveryId <= 0) {
            throw new InvalidArgumentException('Delivery ID must be positive.');
        }
    }
}
