<?php

declare(strict_types=1);

namespace FluxQ\Support;

use RuntimeException;

final readonly class Dotenv
{
    public static function load(string $path): void
    {
        if (!is_file($path)) {
            return;
        }

        $lines = file($path, FILE_IGNORE_NEW_LINES);

        if ($lines === false) {
            throw new RuntimeException(sprintf('Could not read environment file: %s', $path));
        }

        foreach ($lines as $line) {
            self::loadLine($line);
        }
    }

    private static function loadLine(string $line): void
    {
        $line = trim($line);

        if ($line === '' || str_starts_with($line, '#')) {
            return;
        }

        if (str_starts_with($line, 'export ')) {
            $line = trim(substr($line, 7));
        }

        [$name, $value] = array_pad(explode('=', $line, 2), 2, null);

        if ($value === null) {
            return;
        }

        $name = trim($name);

        if (!preg_match('/^[A-Z_][A-Z0-9_]*$/', $name)) {
            return;
        }

        if (getenv($name) !== false) {
            return;
        }

        $value = self::parseValue(trim($value));

        putenv($name . '=' . $value);
        $_ENV[$name] = $value;
        $_SERVER[$name] = $value;
    }

    private static function parseValue(string $value): string
    {
        if ($value === '') {
            return '';
        }

        $first = $value[0];
        $last = $value[strlen($value) - 1];

        if (($first === '"' && $last === '"') || ($first === "'" && $last === "'")) {
            $value = substr($value, 1, -1);

            if ($first === '"') {
                return strtr($value, [
                    '\\n' => "\n",
                    '\\r' => "\r",
                    '\\t' => "\t",
                    '\\"' => '"',
                    '\\\\' => '\\',
                ]);
            }

            return $value;
        }

        $commentPosition = strpos($value, ' #');

        if ($commentPosition !== false) {
            $value = substr($value, 0, $commentPosition);
        }

        return rtrim($value);
    }
}
