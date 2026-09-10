<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use FluxQ\Persistence\Postgres\UserRepository;

final readonly class Authorizer implements AuthorizationService
{
    public function __construct(
        private UserRepository $users
    ) {
    }

    public function authorize(
        AuthenticatedUser $user,
        string $virtualHost,
        AuthorizationPermission $permission,
        string $resource
    ): AuthorizationResult {
        $permissions = $this->users->permissionsForUserVirtualHost($user->id, $virtualHost);
        if ($permissions === null) {
            return AuthorizationResult::deny();
        }

        $pattern = match ($permission) {
            AuthorizationPermission::Configure => $permissions->configurePattern,
            AuthorizationPermission::Write => $permissions->writePattern,
            AuthorizationPermission::Read => $permissions->readPattern,
        };

        return ResourcePermissionMatcher::matches($pattern, $resource)
            ? AuthorizationResult::allow()
            : AuthorizationResult::deny();
    }
}
