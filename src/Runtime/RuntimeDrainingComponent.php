<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

interface RuntimeDrainingComponent extends RuntimeComponent
{
    public function beginDrain(): void;

    public function inFlightCount(): int;
}
