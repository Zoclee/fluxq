<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use Throwable;

final readonly class VhostListCommand
{
    public function __construct(
        private ReadOnlyDatabaseContext $context
    ) {
    }

    /**
     * @param resource $output
     */
    public function run(mixed $output): int
    {
        try {
            $virtualHosts = (new VirtualHostRepository($this->context->connect()))->all();
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $this->context->safeError($exception)));

            return 1;
        }

        if ($virtualHosts === []) {
            $this->write($output, "No virtual hosts found.\n");

            return 0;
        }

        $rows = array_map(
            static fn ($virtualHost): array => [(string) $virtualHost->id, $virtualHost->name],
            $virtualHosts
        );

        $this->write($output, "Virtual Hosts\n\n");
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
