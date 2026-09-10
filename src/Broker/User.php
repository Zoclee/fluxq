<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class User
{
    public function __construct(
        public int $id,
        public string $username,
        public string $passwordHash,
        public bool $enabled,
        public DateTimeImmutable $createdAt,
        public DateTimeImmutable $updatedAt
    ) {
    }
}
