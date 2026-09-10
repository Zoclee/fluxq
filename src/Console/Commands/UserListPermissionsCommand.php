<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\UserRepository;
use Throwable;

final readonly class UserListPermissionsCommand
{
    public function __construct(
        private Connection $connection
    ) {
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    public function run(array $arguments, mixed $output): int
    {
        $username = $arguments[0] ?? '';
        if ($username === '') {
            $this->write($output, "Usage: fluxq user:list-permissions <username>\n");

            return 1;
        }

        try {
            $permissions = (new UserRepository($this->connection))->permissionsForUsername($username);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        if ($permissions === []) {
            $this->write($output, sprintf("No permissions found for user \"%s\".\n", $username));

            return 0;
        }

        $rows = array_map(
            static fn ($permission): array => [
                $permission->virtualHost,
                $permission->configurePattern,
                $permission->writePattern,
                $permission->readPattern,
            ],
            $permissions
        );

        $this->write($output, sprintf("Permissions for %s\n\n", $username));
        $this->write($output, Table::render(['Vhost', 'Configure', 'Write', 'Read'], $rows));
        $this->write($output, "\n");

        return 0;
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }
}
