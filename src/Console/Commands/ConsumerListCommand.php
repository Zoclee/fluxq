<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Runtime\RuntimeDiagnostics;
use RuntimeException;

final readonly class ConsumerListCommand
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
            $consumers = $this->diagnostics->consumers();
        } catch (RuntimeException) {
            $this->write($output, "Runtime: unavailable\n");

            return 1;
        }

        if ($consumers === []) {
            $this->write($output, "Consumers\n\nNo active consumers.\n");

            return 0;
        }

        $rows = array_map(
            static fn (array $consumer): array => [
                (string) ($consumer['id'] ?? ''),
                (string) ($consumer['connection_id'] ?? ''),
                (string) ($consumer['virtual_host'] ?? ''),
                (string) ($consumer['destination'] ?? ''),
                (string) ($consumer['subscription'] ?? ''),
                (string) ($consumer['created_at'] ?? ''),
            ],
            $consumers
        );

        $this->write($output, "Consumers\n\n");
        $this->write($output, Table::render(['ID', 'Connection', 'Virtual host', 'Destination', 'Subscription', 'Created at'], $rows));

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
