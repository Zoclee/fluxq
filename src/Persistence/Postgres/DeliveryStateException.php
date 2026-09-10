<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use RuntimeException;

final class DeliveryStateException extends RuntimeException
{
    public static function notFound(int $id): self
    {
        return new self(sprintf('Delivery %d does not exist.', $id));
    }

    public static function invalidTransition(int $id, string $from, string $to): self
    {
        return new self(sprintf('Delivery %d cannot transition from %s to %s.', $id, $from, $to));
    }
}
