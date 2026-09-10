<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use RuntimeException;

final readonly class ConnectionConfig
{
    public function __construct(
        public string $host,
        public int $port,
        public string $database,
        public string $user,
        public ?string $password
    ) {
        if ($this->host === '') {
            throw new RuntimeException('FLUXQ_DB_HOST is not configured.');
        }

        if ($this->port <= 0) {
            throw new RuntimeException('FLUXQ_DB_PORT must be a positive integer.');
        }

        if ($this->database === '') {
            throw new RuntimeException('FLUXQ_DB_NAME is not configured.');
        }

        if ($this->user === '') {
            throw new RuntimeException('FLUXQ_DB_USER is not configured.');
        }
    }

    /**
     * @param array{
     *     host?: string,
     *     port?: int,
     *     name?: string,
     *     user?: string,
     *     password?: string|null
     * } $config
     */
    public static function fromArray(array $config): self
    {
        return new self(
            self::stringValue($config, 'host', 'FLUXQ_DB_HOST'),
            (int) ($config['port'] ?? 0),
            self::stringValue($config, 'name', 'FLUXQ_DB_NAME'),
            self::stringValue($config, 'user', 'FLUXQ_DB_USER'),
            isset($config['password']) && is_string($config['password']) ? $config['password'] : null
        );
    }

    public function dsn(): string
    {
        return sprintf('pgsql:host=%s;port=%d;dbname=%s', $this->host, $this->port, $this->database);
    }

    public function redact(string $message): string
    {
        if ($this->password === null || $this->password === '') {
            return $message;
        }

        return str_replace($this->password, '[redacted]', $message);
    }

    /**
     * @param array<string, mixed> $config
     */
    private static function stringValue(array $config, string $key, string $environmentName): string
    {
        $value = $config[$key] ?? null;

        if (!is_string($value) || $value === '') {
            throw new RuntimeException(sprintf('%s is not configured.', $environmentName));
        }

        return $value;
    }
}
