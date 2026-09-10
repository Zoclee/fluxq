<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\UserRepository;
use Throwable;

final readonly class UserListCommand
{
    public function __construct(
        private Connection $connection
    ) {
    }

    /**
     * @param resource $output
     */
    public function run(mixed $output): int
    {
        try {
            $users = (new UserRepository($this->connection))->all();
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        if ($users === []) {
            $this->write($output, "No users found.\n");

            return 0;
        }

        $rows = array_map(
            static fn ($user): array => [(string) $user->id, $user->username, $user->enabled ? 'yes' : 'no'],
            $users
        );

        $this->write($output, "Users\n\n");
        $this->write($output, Table::render(['ID', 'Username', 'Enabled'], $rows));
        $this->write($output, sprintf("\n%d user%s.\n", count($rows), count($rows) === 1 ? '' : 's'));

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
