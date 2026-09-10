<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Runtime\RuntimeDiagnostics;
use RuntimeException;

final readonly class ConnectionListCommand
{
    public function __construct(
        private RuntimeDiagnostics $diagnostics
    ) {
    }

    /**
     * @param resource $output
     */
    public function run(mixed $output): int
    {
        try {
            $connections = $this->diagnostics->connections();
        } catch (RuntimeException) {
            $this->write($output, "Runtime: unavailable\n");

            return 1;
        }

        if ($connections === []) {
            $this->write($output, "Connections\n\nNo active connections.\n");

            return 0;
        }

        $rows = array_map(
            static fn (array $connection): array => [
                (string) ($connection['id'] ?? ''),
                (string) ($connection['protocol'] ?? ''),
                (string) ($connection['remote_address'] ?? ''),
                (string) ($connection['connected_at'] ?? ''),
            ],
            $connections
        );

        $this->write($output, "Connections\n\n");
        $this->write($output, Table::render(['ID', 'Protocol', 'Remote address', 'Connected at'], $rows));

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
