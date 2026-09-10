<?php

declare(strict_types=1);

namespace FluxQ\Console\Commands;

use FluxQ\Broker\DestinationType;
use FluxQ\Console\Table;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use Throwable;

final readonly class MessagePeekCommand
{
    private const DEFAULT_LIMIT = 10;
    private const MAX_LIMIT = 100;
    private const PREVIEW_BYTES = 80;

    public function __construct(
        private ReadOnlyDatabaseContext $context
    ) {
    }

    /**
     * @param list<string> $arguments
     * @param resource $output
     */
    public function run(array $arguments, mixed $output): int
    {
        $queueName = $arguments[0] ?? null;

        if ($queueName === null || $queueName === '') {
            $this->write($output, "ERROR: message:peek requires a queue name.\n");

            return 1;
        }

        $limit = self::DEFAULT_LIMIT;
        if (isset($arguments[1])) {
            if (!ctype_digit($arguments[1]) || (int) $arguments[1] < 1) {
                $this->write($output, "ERROR: message:peek limit must be a positive integer.\n");

                return 1;
            }

            $limit = min((int) $arguments[1], self::MAX_LIMIT);
        }

        try {
            $connection = $this->context->connect();
            $virtualHost = $this->context->defaultVirtualHost($connection);
            $queue = (new DestinationRepository($connection))->findByName($virtualHost->id, $queueName);

            if ($queue === null || $queue->type !== DestinationType::Queue) {
                $this->write($output, sprintf("ERROR: Queue \"%s\" was not found in virtual host \"%s\".\n", $queueName, $virtualHost->name));

                return 1;
            }

            $routes = (new MessageRouteRepository($connection))->peekByDestination($queue->id, $limit);
            $messages = (new MessageRepository($connection))->findByIds(
                array_map(static fn ($route): int => $route->messageId, $routes)
            );
        } catch (Throwable $exception) {
            $this->write($output, sprintf("ERROR: %s\n", $this->context->safeError($exception)));

            return 1;
        }

        $messageById = [];
        foreach ($messages as $message) {
            $messageById[$message->id] = $message;
        }

        $this->write($output, sprintf("Queue: %s\n\n", $queue->name));

        if ($routes === []) {
            $this->write($output, "No messages found.\n");

            return 0;
        }

        $rows = array_map(
            static function ($route) use ($messageById): array {
                $message = $messageById[$route->messageId] ?? null;

                return [
                    (string) $route->id,
                    $message?->messageId ?? (string) $route->messageId,
                    $route->availableAt->format('Y-m-d H:i:sP'),
                    $route->expiresAt?->format('Y-m-d H:i:sP') ?? '-',
                    $message === null ? '-' : self::preview($message->payload),
                ];
            },
            $routes
        );

        $this->write($output, Table::render(['Route', 'Message UUID', 'Available', 'Expires', 'Preview'], $rows));
        $this->write($output, sprintf("\n%d %s shown.\n", count($rows), count($rows) === 1 ? 'message' : 'messages'));

        return 0;
    }

    private static function preview(string $payload): string
    {
        $length = strlen($payload);

        if (!self::isPrintableAscii($payload)) {
            return sprintf('<binary: %d bytes>', $length);
        }

        $preview = substr($payload, 0, self::PREVIEW_BYTES);
        $preview = str_replace(["\r", "\n", "\t"], ['\r', '\n', '\t'], $preview);

        return $length > self::PREVIEW_BYTES ? $preview . '...' : $preview;
    }

    private static function isPrintableAscii(string $payload): bool
    {
        for ($index = 0, $length = strlen($payload); $index < $length; $index++) {
            $byte = ord($payload[$index]);

            if ($byte === 9 || $byte === 10 || $byte === 13) {
                continue;
            }

            if ($byte < 32 || $byte > 126) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param resource $output
     */
    private function write(mixed $output, string $message): void
    {
        fwrite($output, $message);
    }
}
