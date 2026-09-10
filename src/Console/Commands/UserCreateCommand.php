<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use Closure;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\UserRepository;
use Throwable;

final readonly class UserCreateCommand
{
    private ?Closure $passwordReader;

    /**
     * @param null|callable(): string $passwordReader
     */
    public function __construct(
        private Connection $connection,
        ?callable $passwordReader = null
    ) {
        $this->passwordReader = $passwordReader === null ? null : Closure::fromCallable($passwordReader);
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    public function run(array $arguments, mixed $output): int
    {
        $username = $arguments[0] ?? '';
        if ($username === '') {
            $this->write($output, "Usage: fluxq user:create <username>\n");

            return 1;
        }

        try {
            $password = $this->readPassword($output);
            if ($password === '') {
                $this->write($output, "ERROR: Password must not be empty.\n");

                return 1;
            }

            (new UserRepository($this->connection))->create($username, $password);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $exception->getMessage()));

            return 1;
        }

        $this->write($output, sprintf("Created user \"%s\".\n", $username));

        return 0;
    }

    /**
     * @param resource $output
     */
    private function readPassword(mixed $output): string
    {
        if ($this->passwordReader !== null) {
            return ($this->passwordReader)();
        }

        $this->write($output, 'Password: ');
        $line = fgets(STDIN);
        $this->write($output, "\n");

        return $line === false ? '' : rtrim($line, "\r\n");
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }
}
