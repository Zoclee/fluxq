<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class UserPermissions
{
    public function __construct(
        public int $userId,
        public string $username,
        public int $virtualHostId,
        public string $virtualHost,
        public string $configurePattern,
        public string $writePattern,
        public string $readPattern,
        public DateTimeImmutable $createdAt,
        public DateTimeImmutable $updatedAt
    ) {
    }
}
