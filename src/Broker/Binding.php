<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class Binding
{
    /**
     * @param array<string, mixed> $metadata
     */
    public function __construct(
        public int $id,
        public int $virtualHostId,
        public string $source,
        public int $destinationId,
        public string $routingKey,
        public array $metadata,
        public DateTimeImmutable $createdAt
    ) {
    }
}
