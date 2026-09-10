<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use Throwable;

final readonly class BindingListCommand
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
            $connection = $this->context->connect();
            $virtualHost = $this->context->defaultVirtualHost($connection);
            $bindings = (new BindingRepository($connection))->allByVirtualHost($virtualHost->id);
            $destinations = (new DestinationRepository($connection))->allByVirtualHost($virtualHost->id);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $this->context->safeError($exception)));

            return 1;
        }

        if ($bindings === []) {
            $this->write($output, "No bindings found.\n");

            return 0;
        }

        $destinationNames = [];
        foreach ($destinations as $destination) {
            $destinationNames[$destination->id] = $destination->name;
        }

        $rows = array_map(
            static fn ($binding): array => [
                (string) $binding->id,
                $binding->source,
                $binding->routingKey,
                $destinationNames[$binding->destinationId] ?? (string) $binding->destinationId,
            ],
            $bindings
        );

        $this->write($output, "Bindings\n\n");
        $this->write($output, Table::render(['ID', 'Source', 'Routing Key', 'Destination'], $rows));
        $this->write($output, sprintf("\n%d %s.\n", count($rows), count($rows) === 1 ? 'binding' : 'bindings'));

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
