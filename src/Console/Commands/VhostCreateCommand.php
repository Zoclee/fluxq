<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDOException;
use Throwable;

final readonly class VhostCreateCommand
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
        $name = $arguments[0] ?? '';

        if (count($arguments) !== 1 || $name === '') {
            $this->write($output, "Usage: fluxq vhost:create <name>\n");

            return 1;
        }

        try {
            (new VirtualHostRepository($this->connection))->create($name);
        } catch (PDOException $exception) {
            if ($exception->getCode() === '23505') {
                $this->write($output, sprintf("ERROR: Virtual host \"%s\" already exists.\n", $name));

                return 1;
            }

            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        $this->write($output, sprintf("Virtual host created: %s\n", $name));

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
