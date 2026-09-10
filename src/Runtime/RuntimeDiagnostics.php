<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

interface RuntimeDiagnostics
{
    /**
     * @return array<string, mixed>
     */
    public function stats(): array;

    /**
     * @return list<array<string, mixed>>
     */
    public function connections(): array;

    /**
     * @return list<array<string, mixed>>
     */
    public function consumers(): array;
}
