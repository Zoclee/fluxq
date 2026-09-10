<?php

declare(strict_types=1);

namespace FluxQ\Broker;

final readonly class TopicMatcher
{
    public function matches(string $pattern, string $routingKey): bool
    {
        $patternWords = $this->words($pattern);
        $routingWords = $this->words($routingKey);

        if ($patternWords === null || $routingWords === null) {
            return false;
        }

        $memo = [];

        return $this->matchFrom($patternWords, $routingWords, 0, 0, $memo);
    }

    /**
     * @param list<string> $patternWords
     * @param list<string> $routingWords
     * @param array<string, bool> $memo
     */
    private function matchFrom(array $patternWords, array $routingWords, int $patternIndex, int $routingIndex, array &$memo): bool
    {
        $memoKey = $patternIndex . ':' . $routingIndex;
        if (array_key_exists($memoKey, $memo)) {
            return $memo[$memoKey];
        }

        if ($patternIndex === count($patternWords)) {
            return $memo[$memoKey] = $routingIndex === count($routingWords);
        }

        $word = $patternWords[$patternIndex];
        if ($word === '#') {
            if ($this->matchFrom($patternWords, $routingWords, $patternIndex + 1, $routingIndex, $memo)) {
                return $memo[$memoKey] = true;
            }

            return $memo[$memoKey] = $routingIndex < count($routingWords)
                && $this->matchFrom($patternWords, $routingWords, $patternIndex, $routingIndex + 1, $memo);
        }

        if ($routingIndex === count($routingWords)) {
            return $memo[$memoKey] = false;
        }

        if ($word === '*' || $word === $routingWords[$routingIndex]) {
            return $memo[$memoKey] = $this->matchFrom(
                $patternWords,
                $routingWords,
                $patternIndex + 1,
                $routingIndex + 1,
                $memo
            );
        }

        return $memo[$memoKey] = false;
    }

    /**
     * @return null|list<string>
     */
    private function words(string $value): ?array
    {
        if ($value === '') {
            return [];
        }

        $words = explode('.', $value);
        foreach ($words as $word) {
            if ($word === '') {
                return null;
            }
        }

        return $words;
    }
}
