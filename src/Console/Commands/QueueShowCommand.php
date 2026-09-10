<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Broker\DestinationType;
use FluxQ\Broker\DeliveryState;
use FluxQ\Broker\RetryPolicy;
use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use Throwable;

final readonly class QueueShowCommand
{
    public function __construct(
        private ReadOnlyDatabaseContext $context
    ) {
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    public function run(array $arguments, mixed $output): int
    {
        $queueName = $arguments[0] ?? null;

        if ($queueName === null || $queueName === '') {
            $this->write($output, "ERROR: queue:show requires a queue name.\n");

            return 1;
        }

        try {
            $connection = $this->context->connect();
            $virtualHost = $this->context->defaultVirtualHost($connection);
            $destinations = new DestinationRepository($connection);
            $queue = $destinations->findByName($virtualHost->id, $queueName);

            if ($queue === null || $queue->type !== DestinationType::Queue) {
                $this->write($output, sprintf("ERROR: Queue \"%s\" was not found in virtual host \"%s\".\n", $queueName, $virtualHost->name));

                return 1;
            }

            $retryPolicy = RetryPolicy::fromDestinationMetadata($queue->metadata);
            $bindingCount = (new BindingRepository($connection))->countByDestination($queue->id);
            $subscriptionCount = (new SubscriptionRepository($connection))->countByDestination($queue->id);
            $routeCount = (new MessageRouteRepository($connection))->countByDestination($queue->id);
            $deliveryCounts = (new DeliveryRepository($connection))->countByStateForDestination($queue->id);
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $this->context->safeError($exception)));

            return 1;
        }

        $this->write($output, sprintf("Queue: %s\n\n", $queue->name));
        $this->write($output, sprintf("ID:             %d\n", $queue->id));
        $this->write($output, sprintf("Virtual Host:   %s\n", $virtualHost->name));
        $this->write($output, sprintf("Type:           %s\n", $queue->type->value));
        $this->write($output, sprintf("Durable:        %s\n", self::yesNo($queue->durable)));
        $this->write($output, sprintf("Auto Delete:    %s\n", self::yesNo($queue->autoDelete)));
        $this->write($output, sprintf("Retry Policy:   %s\n", $retryPolicy === null ? 'none' : sprintf(
            'max_attempts=%d, retry_delay_seconds=%d',
            $retryPolicy->maxAttempts,
            $retryPolicy->retryDelaySeconds
        )));
        $this->write($output, sprintf("Dead Letter:    %s\n", $retryPolicy?->deadLetterDestination ?? 'none'));
        $this->write($output, sprintf("Created:        %s\n", $queue->createdAt->format('Y-m-d H:i:sP')));
        $this->write($output, sprintf("Updated:        %s\n\n", $queue->updatedAt->format('Y-m-d H:i:sP')));
        $this->write($output, sprintf("Bindings:       %d\n", $bindingCount));
        $this->write($output, sprintf("Subscriptions:  %d\n", $subscriptionCount));
        $this->write($output, sprintf("Routes:         %d\n", $routeCount));
        $this->write($output, sprintf("Pending:        %d\n", self::deliveryCount($deliveryCounts, DeliveryState::Pending)));
        $this->write($output, sprintf("Reserved:       %d\n", self::deliveryCount($deliveryCounts, DeliveryState::Reserved)));
        $this->write($output, sprintf(
            "Depth:          %d\n",
            self::deliveryCount($deliveryCounts, DeliveryState::Pending) + self::deliveryCount($deliveryCounts, DeliveryState::Reserved)
        ));
        $this->write($output, sprintf("Acknowledged:   %d\n", self::deliveryCount($deliveryCounts, DeliveryState::Acknowledged)));
        $this->write($output, sprintf("Rejected:       %d\n", self::deliveryCount($deliveryCounts, DeliveryState::Rejected)));

        return 0;
    }

    /**
     * @param array<string, int> $counts
     */
    private static function deliveryCount(array $counts, DeliveryState $state): int
    {
        return $counts[$state->value] ?? 0;
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
