<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use RuntimeException;
use Throwable;

final class MigrationFailure extends RuntimeException
{
    public function __construct(
        public readonly string $migration,
        Throwable $previous
    ) {
        parent::__construct(
            sprintf('Migration failed: %s', $migration),
            0,
            $previous
        );
    }
}
