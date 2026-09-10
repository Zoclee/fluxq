<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\UserRepository;
use Throwable;

final readonly class UserListVhostsCommand
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
        if (count($arguments) !== 1 || ($arguments[0] ?? '') === '') {
            $this->write($output, "Usage: fluxq user:list-vhosts <username>\n");

            return 1;
        }

        $username = $arguments[0];

        try {
            $repository = new UserRepository($this->connection);
            $user = $repository->findByUsername($username);

            if ($user === null) {
                $this->write($output, sprintf("ERROR: User \"%s\" was not found.\n", $username));

                return 1;
            }

            $virtualHosts = $repository->listVirtualHosts($user->id);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        if ($virtualHosts === []) {
            $this->write($output, sprintf("No virtual-host grants found for user \"%s\".\n", $username));

            return 0;
        }

        $rows = array_map(
            static fn ($virtualHost): array => [(string) $virtualHost->id, $virtualHost->name],
            $virtualHosts
        );

        $this->write($output, sprintf("Virtual Hosts for user \"%s\"\n\n", $username));
        $this->write($output, Table::render(['ID', 'Name'], $rows));
        $this->write($output, sprintf("\n%d virtual %s.\n", count($rows), count($rows) === 1 ? 'host' : 'hosts'));

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
