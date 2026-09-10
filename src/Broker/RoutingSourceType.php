<?php

declare(strict_types=1);

namespace FluxQ\Broker;

enum RoutingSourceType: string
{
    case Direct = 'direct';
    case Fanout = 'fanout';
    case Topic = 'topic';
}
