<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class Subscription
{
    /**
     * @param array<string, mixed> $metadata
     */
    public function __construct(
        public int $id,
        public int $destinationId,
        public string $name,
        public bool $durable,
        public array $metadata,
        public DateTimeImmutable $createdAt,
        public DateTimeImmutable $updatedAt
    ) {
    }
}
