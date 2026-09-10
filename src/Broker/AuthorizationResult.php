<?php

declare(strict_types=1);

namespace FluxQ\Broker;

final readonly class AuthorizationResult
{
    private function __construct(
        public bool $allowed
    ) {
    }

    public static function allow(): self
    {
        return new self(true);
    }

    public static function deny(): self
    {
        return new self(false);
    }
}
