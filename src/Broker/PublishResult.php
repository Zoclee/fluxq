<?php

declare(strict_types=1);

namespace FluxQ\Broker;

final readonly class PublishResult
{
    /**
     * @param list<MessageRoute> $routes
     * @param list<Delivery> $deliveries
     */
    public function __construct(
        public ?Message $message,
        public array $routes,
        public array $deliveries
    ) {
    }

    public function messageId(): ?string
    {
        return $this->message?->messageId;
    }

    public function routeCount(): int
    {
        return count($this->routes);
    }

    public function deliveryCount(): int
    {
        return count($this->deliveries);
    }
}
