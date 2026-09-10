<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class VirtualHost
{
    public function __construct(
        public int $id,
        public string $name,
        public DateTimeImmutable $createdAt
    ) {
    }
}
