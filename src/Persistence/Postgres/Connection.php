<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use PDO;
use PDOException;
use RuntimeException;
use Throwable;

final class Connection
{
    private ?PDO $pdo = null;

    public function __construct(
        private readonly ConnectionConfig $config
    ) {
    }

    public static function fromConfig(ConnectionConfig $config): self
    {
        return new self($config);
    }

    public static function fromDsn(string $dsn): self
    {
        if (!extension_loaded('pdo_pgsql')) {
            throw new RuntimeException('The pdo_pgsql extension is required to connect to PostgreSQL.');
        }

        if ($dsn === '') {
            throw new RuntimeException('PostgreSQL DSN must not be empty.');
        }

        return new self(self::configFromDsn($dsn));
    }

    public function pdo(): PDO
    {
        if ($this->pdo !== null) {
            return $this->pdo;
        }

        if (!extension_loaded('pdo_pgsql')) {
            throw new RuntimeException('The pdo_pgsql extension is required to connect to PostgreSQL.');
        }

        try {
            $this->pdo = new PDO($this->config->dsn(), $this->config->user, $this->config->password, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]);

            return $this->pdo;
        } catch (PDOException $exception) {
            throw new RuntimeException(
                sprintf('Could not connect to PostgreSQL: %s', $this->config->redact($exception->getMessage())),
                0,
                $exception
            );
        }
    }

    /**
     * @template T
     * @param callable(PDO): T $callback
     * @return T
     */
    public function transaction(callable $callback): mixed
    {
        $pdo = $this->pdo();

        if ($pdo->inTransaction()) {
            throw new RuntimeException('Nested PostgreSQL transactions are not supported.');
        }

        try {
            $pdo->beginTransaction();
            $result = $callback($pdo);
            $pdo->commit();

            return $result;
        } catch (Throwable $exception) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }

            throw $exception;
        }
    }

    public function config(): ConnectionConfig
    {
        return $this->config;
    }

    private static function configFromDsn(string $dsn): ConnectionConfig
    {
        $parts = [];

        foreach (explode(';', preg_replace('/^pgsql:/', '', $dsn) ?? '') as $part) {
            if ($part === '' || !str_contains($part, '=')) {
                continue;
            }

            [$key, $value] = explode('=', $part, 2);
            $parts[$key] = $value;
        }

        return new ConnectionConfig(
            (string) ($parts['host'] ?? '127.0.0.1'),
            (int) ($parts['port'] ?? 5432),
            (string) ($parts['dbname'] ?? ''),
            (string) ($parts['user'] ?? ''),
            isset($parts['password']) ? (string) $parts['password'] : null
        );
    }
}
