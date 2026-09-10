<?php

declare(strict_types=1);

namespace FluxQ\Broker;

interface AuthenticationService
{
    public function authenticate(string $username, string $password): AuthenticationResult;

    public function canAccessVirtualHost(AuthenticatedUser $user, string $virtualHost): bool;
}
