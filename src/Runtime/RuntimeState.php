<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

enum RuntimeState: string
{
    case Created = 'created';
    case Starting = 'starting';
    case Running = 'running';
    case Draining = 'draining';
    case Stopping = 'stopping';
    case Stopped = 'stopped';
}
