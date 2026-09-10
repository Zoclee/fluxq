<?php

declare(strict_types=1);

namespace FluxQ\Broker;

final readonly class AuthenticationResult
{
    private function __construct(
        public bool $authenticated,
        public ?AuthenticatedUser $user = null
    ) {
    }

    public static function success(AuthenticatedUser $user): self
    {
        return new self(true, $user);
    }

    public static function failure(): self
    {
        return new self(false);
    }
}
