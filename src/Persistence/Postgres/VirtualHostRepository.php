<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\VirtualHost;
use PDO;
use RuntimeException;

final readonly class VirtualHostRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    public function findByName(string $name): ?VirtualHost
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, name, created_at
FROM virtual_hosts
WHERE name = :name
SQL);
        $statement->execute(['name' => $name]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    public function create(string $name): VirtualHost
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO virtual_hosts (name)
VALUES (:name)
RETURNING id, name, created_at
SQL);
        $statement->execute(['name' => $name]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created virtual host.');
        }

        return $this->mapRow($row);
    }

    /**
     * @return list<VirtualHost>
     */
    public function all(): array
    {
        $statement = $this->connection->pdo()->query(<<<'SQL'
SELECT id, name, created_at
FROM virtual_hosts
ORDER BY name
SQL);

        if ($statement === false) {
            throw new RuntimeException('Could not read virtual hosts from PostgreSQL.');
        }

        return array_map(
            fn (array $row): VirtualHost => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function countAll(): int
    {
        $statement = $this->connection->pdo()->query('SELECT COUNT(*) FROM virtual_hosts');

        if ($statement === false) {
            throw new RuntimeException('Could not count virtual hosts in PostgreSQL.');
        }

        return (int) $statement->fetchColumn();
    }

    /**
     * @param array{id: mixed, name: mixed, created_at: mixed} $row
     */
    private function mapRow(array $row): VirtualHost
    {
        return new VirtualHost(
            (int) $row['id'],
            (string) $row['name'],
            new DateTimeImmutable((string) $row['created_at'])
        );
    }
}
