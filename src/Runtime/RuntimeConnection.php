<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

use DateTimeImmutable;
use FluxQ\Support\Uuid;
use InvalidArgumentException;

final readonly class RuntimeConnection
{
    /**
     * @param array<string, mixed> $metadata
     */
    public function __construct(
        public string $id,
        public string $protocol,
        public DateTimeImmutable $connectedAt,
        public ?string $remoteAddress = null,
        public array $metadata = []
    ) {
        Uuid::assertValid($this->id, 'Connection ID');

        if ($this->protocol === '') {
            throw new InvalidArgumentException('Connection protocol must not be empty.');
        }

        if ($this->metadata !== [] && array_is_list($this->metadata)) {
            throw new InvalidArgumentException('Connection metadata must be a JSON object.');
        }
    }

    /**
     * @param array<string, mixed> $metadata
     */
    public static function create(
        string $protocol,
        ?string $remoteAddress = null,
        array $metadata = []
    ): self {
        return new self(Uuid::v4(), $protocol, new DateTimeImmutable(), $remoteAddress, $metadata);
    }
}
