<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Runtime\RuntimeDiagnostics;
use RuntimeException;

final readonly class HealthCommand
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
        $this->write($output, "FluxQ Health\n\n");

        try {
            $stats = $this->diagnostics->stats();
        } catch (RuntimeException) {
            $this->write($output, "Runtime: unavailable\n");

            return 1;
        }

        $state = (string) ($stats['state'] ?? 'unknown');

        if ($state !== 'running') {
            $this->write($output, "Runtime: unhealthy\n");
            $this->write($output, sprintf("State:   %s\n", self::displayState($state)));

            return 1;
        }

        $this->write($output, "Runtime: healthy\n");
        $this->write($output, "State:   Running\n");

        return 0;
    }

    private static function displayState(string $state): string
    {
        return match ($state) {
            'created' => 'Created',
            'starting' => 'Starting',
            'running' => 'Running',
            'draining' => 'Draining',
            'stopping' => 'Stopping',
            'stopped' => 'Stopped',
            default => 'Unknown',
        };
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }
}
