<?php

declare(strict_types=1);

namespace FluxQ\Broker;

final readonly class ResourcePermissionMatcher
{
    public static function matches(string $pattern, string $resource): bool
    {
        if ($pattern === '') {
            return false;
        }

        return preg_match(self::delimitedPattern($pattern), $resource) === 1;
    }

    public static function isValid(string $pattern): bool
    {
        set_error_handler(static fn (): bool => true);
        try {
            return preg_match(self::delimitedPattern($pattern), '') !== false;
        } finally {
            restore_error_handler();
        }
    }

    private static function delimitedPattern(string $pattern): string
    {
        return '~' . str_replace('~', '\\~', $pattern) . '~u';
    }
}
