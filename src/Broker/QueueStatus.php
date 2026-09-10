<?php

declare(strict_types=1);

namespace FluxQ\Broker;

final readonly class QueueStatus
{
    public function __construct(
        public Destination $destination,
        public int $messageCount
    ) {
    }
}
