<?php

declare(strict_types=1);

namespace FluxQ\Broker;

enum DeliveryState: string
{
    case Pending = 'pending';
    case Reserved = 'reserved';
    case Acknowledged = 'acknowledged';
    case Rejected = 'rejected';
}
