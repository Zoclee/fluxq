<?php

declare(strict_types=1);

namespace FluxQ\Protocol\Amqp;

enum AmqpConnectionState: string
{
    case AwaitingProtocolHeader = 'awaiting_protocol_header';
    case Starting = 'starting';
    case Tuning = 'tuning';
    case Opening = 'opening';
    case Open = 'open';
    case Closing = 'closing';
    case Closed = 'closed';
}
