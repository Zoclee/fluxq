<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

use DateTimeImmutable;
use FluxQ\Support\Uuid;
use InvalidArgumentException;

final readonly class RuntimeConsumer
{
    /**
     * @param array<string, mixed> $metadata
     */
    public function __construct(
        public string $id,
        public string $connectionId,
        public string $virtualHost,
        public string $destination,
        public string $subscription,
        public DateTimeImmutable $createdAt,
        public bool $exclusive = false,
        public array $metadata = []
    ) {
        Uuid::assertValid($this->id, 'Consumer ID');
        Uuid::assertValid($this->connectionId, 'Connection ID');

        if ($this->virtualHost === '') {
            throw new InvalidArgumentException('Consumer virtual host must not be empty.');
        }

        if ($this->destination === '') {
            throw new InvalidArgumentException('Consumer destination must not be empty.');
        }

        if ($this->subscription === '') {
            throw new InvalidArgumentException('Consumer subscription must not be empty.');
        }

        if ($this->metadata !== [] && array_is_list($this->metadata)) {
            throw new InvalidArgumentException('Consumer metadata must be a JSON object.');
        }
    }

    /**
     * @param array<string, mixed> $metadata
     */
    public static function create(
        string $connectionId,
        string $virtualHost,
        string $destination,
        string $subscription,
        bool $exclusive = false,
        array $metadata = []
    ): self {
        return new self(
            Uuid::v4(),
            $connectionId,
            $virtualHost,
            $destination,
            $subscription,
            new DateTimeImmutable(),
            $exclusive,
            $metadata
        );
    }
}
