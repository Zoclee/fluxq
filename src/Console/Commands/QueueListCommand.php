<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Broker\DestinationType;
use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\DestinationRepository;
use Throwable;

final readonly class QueueListCommand
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
            $destinations = (new DestinationRepository($connection))->allByVirtualHost($virtualHost->id);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $this->context->safeError($exception)));

            return 1;
        }

        $queues = array_values(array_filter(
            $destinations,
            static fn ($destination): bool => $destination->type === DestinationType::Queue
        ));

        if ($queues === []) {
            $this->write($output, "No queues found.\n");

            return 0;
        }

        $rows = array_map(
            static fn ($queue): array => [
                (string) $queue->id,
                $queue->name,
                self::yesNo($queue->durable),
                self::yesNo($queue->autoDelete),
            ],
            $queues
        );

        $this->write($output, "Queues\n\n");
        $this->write($output, Table::render(['ID', 'Name', 'Durable', 'Auto Delete'], $rows));
        $this->write($output, sprintf("\n%d %s.\n", count($rows), count($rows) === 1 ? 'queue' : 'queues'));

        return 0;
    }

    private static function yesNo(bool $value): string
    {
        return $value ? 'yes' : 'no';
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }
}
