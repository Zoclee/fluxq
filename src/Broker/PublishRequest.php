<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use FluxQ\Support\Uuid;
use InvalidArgumentException;

final readonly class PublishRequest
{
    /**
     * @param array<string, mixed> $headers
     * @param array<string, mixed> $metadata
     */
    public function __construct(
        public string $virtualHost,
        public string $source,
        public string $routingKey,
        public string $payload,
        public array $headers = [],
        public ?string $contentType = null,
        public ?string $contentEncoding = null,
        public int $priority = 0,
        public bool $persistent = true,
        public ?string $messageId = null,
        public array $metadata = [],
        public bool $persistUnrouted = true
    ) {
        if ($this->virtualHost === '') {
            throw new InvalidArgumentException('Virtual host must not be empty.');
        }

        if ($this->source === '') {
            throw new InvalidArgumentException('Routing source must not be empty.');
        }

        if ($this->headers !== [] && array_is_list($this->headers)) {
            throw new InvalidArgumentException('Message headers must be a JSON object.');
        }

        if ($this->metadata !== [] && array_is_list($this->metadata)) {
            throw new InvalidArgumentException('Message metadata must be a JSON object.');
        }

        if ($this->priority < 0 || $this->priority > 255) {
            throw new InvalidArgumentException('Message priority must be between 0 and 255.');
        }

        if ($this->messageId !== null) {
            Uuid::assertValid($this->messageId, 'Message ID');
        }
    }
}
