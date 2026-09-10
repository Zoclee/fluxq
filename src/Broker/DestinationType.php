<?php

declare(strict_types=1);

namespace FluxQ\Broker;

enum DestinationType: string
{
    case Queue = 'queue';
}
