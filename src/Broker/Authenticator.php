<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use FluxQ\Persistence\Postgres\UserRepository;

final readonly class Authenticator implements AuthenticationService
{
    public function __construct(
        private UserRepository $users
    ) {
    }

    public function authenticate(string $username, string $password): AuthenticationResult
    {
        $user = $this->users->findByUsername($username);

        if ($user === null || !$user->enabled || !password_verify($password, $user->passwordHash)) {
            return AuthenticationResult::failure();
        }

        return AuthenticationResult::success(new AuthenticatedUser($user->id, $user->username));
    }

    public function canAccessVirtualHost(AuthenticatedUser $user, string $virtualHost): bool
    {
        return $this->users->hasVirtualHostAccess($user->id, $virtualHost);
    }
}
