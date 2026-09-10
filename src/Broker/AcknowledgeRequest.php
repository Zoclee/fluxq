<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use InvalidArgumentException;

final readonly class AcknowledgeRequest
{
    public function __construct(
        public int $deliveryId
    ) {
        if ($this->deliveryId <= 0) {
            throw new InvalidArgumentException('Delivery ID must be positive.');
        }
    }
}
