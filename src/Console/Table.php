<?php

declare(strict_types=1);

namespace FluxQ\Console;

final class Table
{
    /**
     * @param list<string> $headers
     * @param list<list<string>> $rows
     */
    public static function render(array $headers, array $rows): string
    {
        $widths = array_map('strlen', $headers);

        foreach ($rows as $row) {
            foreach ($row as $index => $value) {
                $widths[$index] = max($widths[$index] ?? 0, strlen($value));
            }
        }

        $lines = [self::renderRow($headers, $widths)];

        foreach ($rows as $row) {
            $lines[] = self::renderRow($row, $widths);
        }

        return implode("\n", $lines) . "\n";
    }

    /**
     * @param list<string> $row
     * @param list<int> $widths
     */
    private static function renderRow(array $row, array $widths): string
    {
        $columns = [];

        foreach ($row as $index => $value) {
            $columns[] = str_pad($value, $widths[$index]);
        }

        return rtrim(implode('   ', $columns));
    }
}
