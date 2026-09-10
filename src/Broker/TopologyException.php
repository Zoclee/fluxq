<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use RuntimeException;

final class TopologyException extends RuntimeException
{
    public const NOT_FOUND = 'not_found';
    public const PRECONDITION_FAILED = 'precondition_failed';
    public const NOT_IMPLEMENTED = 'not_implemented';
    public const RESOURCE_LOCKED = 'resource_locked';
    public const ACCESS_REFUSED = 'access_refused';

    public function __construct(
        string $message,
        public readonly string $reason
    ) {
        parent::__construct($message);
    }
}
