<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use RuntimeException;

final class SubscriptionNotFoundException extends RuntimeException
{
    public static function forName(string $destination, string $subscription): self
    {
        return new self(sprintf(
            'Subscription "%s" does not exist for destination "%s".',
            $subscription,
            $destination
        ));
    }
}
