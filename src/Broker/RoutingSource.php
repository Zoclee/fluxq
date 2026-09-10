<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class RoutingSource
{
    /**
     * @param array<string, mixed> $metadata
     */
    public function __construct(
        public int $id,
        public int $virtualHostId,
        public string $name,
        public RoutingSourceType $type,
        public bool $durable,
        public bool $autoDelete,
        public array $metadata,
        public DateTimeImmutable $createdAt,
        public DateTimeImmutable $updatedAt
    ) {
    }
}
