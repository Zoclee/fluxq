<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\UserRepository;
use Throwable;

final readonly class UserSetPermissionsCommand
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
        $virtualHost = $arguments[1] ?? '';
        $configure = $arguments[2] ?? null;
        $write = $arguments[3] ?? null;
        $read = $arguments[4] ?? null;

        if ($username === '' || $virtualHost === '' || $configure === null || $write === null || $read === null) {
            $this->write($output, "Usage: fluxq user:set-permissions <username> <vhost> <configure> <write> <read>\n");

            return 1;
        }

        try {
            (new UserRepository($this->connection))->setPermissions($username, $virtualHost, $configure, $write, $read);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        $this->write($output, sprintf("Set permissions for user \"%s\" on virtual host \"%s\".\n", $username, $virtualHost));

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
