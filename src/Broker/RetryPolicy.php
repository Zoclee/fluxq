<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use InvalidArgumentException;

final readonly class RetryPolicy
{
    public function __construct(
        public int $maxAttempts,
        public int $retryDelaySeconds,
        public ?string $deadLetterDestination
    ) {
        if ($this->maxAttempts < 1) {
            throw new InvalidArgumentException('Retry policy max attempts must be at least 1.');
        }

        if ($this->retryDelaySeconds < 0) {
            throw new InvalidArgumentException('Retry policy delay must not be negative.');
        }

        if ($this->deadLetterDestination === '') {
            throw new InvalidArgumentException('Retry policy dead-letter destination must not be empty.');
        }
    }

    /**
     * @param array<string, mixed> $metadata
     */
    public static function fromDestinationMetadata(array $metadata): ?self
    {
        $policy = $metadata['retry_policy'] ?? null;
        if ($policy === null) {
            return null;
        }

        if (!is_array($policy) || array_is_list($policy)) {
            throw new InvalidArgumentException('Destination retry_policy metadata must be an object.');
        }

        $maxAttempts = $policy['max_attempts'] ?? null;
        $retryDelaySeconds = $policy['retry_delay_seconds'] ?? 0;
        $deadLetterDestination = $policy['dead_letter_destination'] ?? null;

        if (!is_int($maxAttempts) && !(is_string($maxAttempts) && ctype_digit($maxAttempts))) {
            throw new InvalidArgumentException('Destination retry_policy.max_attempts must be an integer.');
        }

        if (!is_int($retryDelaySeconds) && !(is_string($retryDelaySeconds) && ctype_digit($retryDelaySeconds))) {
            throw new InvalidArgumentException('Destination retry_policy.retry_delay_seconds must be an integer.');
        }

        if ($deadLetterDestination !== null && !is_string($deadLetterDestination)) {
            throw new InvalidArgumentException('Destination retry_policy.dead_letter_destination must be a string.');
        }

        return new self((int) $maxAttempts, (int) $retryDelaySeconds, $deadLetterDestination);
    }
}
