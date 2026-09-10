<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use InvalidArgumentException;

final readonly class ReserveRequest
{
    public function __construct(
        public string $virtualHost,
        public string $destination,
        public string $subscription,
        public string $consumerId,
        public ?string $deliveryTag = null,
        public ?string $connectionId = null
    ) {
        if ($this->virtualHost === '') {
            throw new InvalidArgumentException('Virtual host must not be empty.');
        }

        if ($this->destination === '') {
            throw new InvalidArgumentException('Destination must not be empty.');
        }

        if ($this->subscription === '') {
            throw new InvalidArgumentException('Subscription must not be empty.');
        }

        if ($this->consumerId === '') {
            throw new InvalidArgumentException('Consumer ID must not be empty.');
        }
    }
}
