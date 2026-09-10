<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use DateTimeImmutable;

final readonly class Message
{
    /**
     * @param array<string, mixed> $headers
     * @param array<string, mixed> $metadata
     */
    public function __construct(
        public int $id,
        public string $messageId,
        public string $payload,
        public array $headers,
        public ?string $contentType,
        public ?string $contentEncoding,
        public int $priority,
        public bool $persistent,
        public array $metadata,
        public DateTimeImmutable $createdAt
    ) {
    }
}
