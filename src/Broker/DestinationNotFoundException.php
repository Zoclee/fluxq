<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use RuntimeException;

final class DestinationNotFoundException extends RuntimeException
{
    public static function forName(string $virtualHost, string $destination): self
    {
        return new self(sprintf(
            'Destination "%s" does not exist in virtual host "%s".',
            $destination,
            $virtualHost
        ));
    }
}
