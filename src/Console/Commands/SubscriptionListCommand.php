<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use Throwable;

final readonly class SubscriptionListCommand
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
            $destinationIds = array_map(static fn ($destination): int => $destination->id, $destinations);
            $subscriptions = (new SubscriptionRepository($connection))->allByDestinations($destinationIds);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $this->context->safeError($exception)));

            return 1;
        }

        if ($subscriptions === []) {
            $this->write($output, "No subscriptions found.\n");

            return 0;
        }

        $destinationNames = [];
        foreach ($destinations as $destination) {
            $destinationNames[$destination->id] = $destination->name;
        }

        $rows = array_map(
            static fn ($subscription): array => [
                (string) $subscription->id,
                $destinationNames[$subscription->destinationId] ?? (string) $subscription->destinationId,
                $subscription->name,
                $subscription->durable ? 'yes' : 'no',
            ],
            $subscriptions
        );

        $this->write($output, "Subscriptions\n\n");
        $this->write($output, Table::render(['ID', 'Destination', 'Name', 'Durable'], $rows));
        $this->write($output, sprintf("\n%d %s.\n", count($rows), count($rows) === 1 ? 'subscription' : 'subscriptions'));

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
