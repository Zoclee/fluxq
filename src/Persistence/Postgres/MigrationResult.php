<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

final readonly class MigrationResult
{
    /**
     * @param list<string> $applied
     */
    public function __construct(
        public array $applied
    ) {
    }

    public function count(): int
    {
        return count($this->applied);
    }
}
