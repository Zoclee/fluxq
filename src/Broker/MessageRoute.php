<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class MessageRoute
{
    public function __construct(
        public int $id,
        public int $messageId,
        public int $destinationId,
        public DateTimeImmutable $availableAt,
        public ?DateTimeImmutable $expiresAt,
        public DateTimeImmutable $createdAt
    ) {
    }
}
