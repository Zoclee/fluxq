<?php

declare(strict_types=1);

namespace FluxQ\Runtime;

use JsonException;
use RuntimeException;

final readonly class RuntimeDiagnosticsClient implements RuntimeDiagnostics
{
    private const MAX_RESPONSE_BYTES = 65536;

    public function __construct(
        private string $host = '127.0.0.1',
        private int $port = 5673,
        private float $timeoutSeconds = 0.25
    ) {
    }

    /**
     * @return array<string, mixed>
     */
    public function stats(): array
    {
        return $this->request('stats');
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function connections(): array
    {
        $data = $this->request('connections');

        return array_is_list($data) ? $data : [];
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function consumers(): array
    {
        $data = $this->request('consumers');

        return array_is_list($data) ? $data : [];
    }

    /**
     * @return array<string, mixed>|list<array<string, mixed>>
     */
    private function request(string $command): array
    {
        $client = @stream_socket_client(
            sprintf('tcp://%s:%d', $this->host, $this->port),
            $errorCode,
            $errorMessage,
            $this->timeoutSeconds
        );

        if ($client === false) {
            throw new RuntimeException('Runtime diagnostics are unavailable.');
        }

        stream_set_timeout($client, (int) $this->timeoutSeconds, (int) (($this->timeoutSeconds - (int) $this->timeoutSeconds) * 1_000_000));
        fwrite($client, json_encode(['command' => $command], JSON_THROW_ON_ERROR) . "\n");

        $response = '';
        while (!feof($client) && strlen($response) <= self::MAX_RESPONSE_BYTES) {
            $chunk = fgets($client, self::MAX_RESPONSE_BYTES + 1);
            if ($chunk === false) {
                break;
            }

            $response .= $chunk;
            if (str_contains($response, "\n")) {
                break;
            }
        }

        fclose($client);

        if ($response === '' || strlen($response) > self::MAX_RESPONSE_BYTES) {
            throw new RuntimeException('Runtime diagnostics returned an invalid response.');
        }

        try {
            $decoded = json_decode(trim($response), true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new RuntimeException('Runtime diagnostics returned malformed JSON.', 0, $exception);
        }

        if (!is_array($decoded) || ($decoded !== [] && array_is_list($decoded)) || ($decoded['ok'] ?? null) !== true) {
            throw new RuntimeException('Runtime diagnostics request failed.');
        }

        $data = $decoded['data'] ?? [];

        return is_array($data) ? $data : [];
    }
}
