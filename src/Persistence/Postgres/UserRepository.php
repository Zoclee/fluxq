<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Broker\ResourcePermissionMatcher;
use FluxQ\Broker\User;
use FluxQ\Broker\UserPermissions;
use FluxQ\Broker\VirtualHost;
use PDO;
use RuntimeException;

final readonly class UserRepository
{
    public function __construct(
        private Connection $connection
    ) {
    }

    public function create(string $username, string $password, bool $enabled = true): User
    {
        $passwordHash = password_hash($password, $this->passwordAlgorithm());
        if (!is_string($passwordHash)) {
            throw new RuntimeException('Could not hash user password.');
        }

        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO users (username, password_hash, enabled)
VALUES (:username, :password_hash, :enabled)
RETURNING id, username, password_hash, enabled, created_at, updated_at
SQL);
        $statement->execute([
            'username' => $username,
            'password_hash' => $passwordHash,
            'enabled' => $enabled,
        ]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        if (!is_array($row)) {
            throw new RuntimeException('PostgreSQL did not return the created user.');
        }

        return $this->mapRow($row);
    }

    public function findByUsername(string $username): ?User
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT id, username, password_hash, enabled, created_at, updated_at
FROM users
WHERE username = :username
SQL);
        $statement->execute(['username' => $username]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapRow($row) : null;
    }

    /**
     * @return list<User>
     */
    public function all(): array
    {
        $statement = $this->connection->pdo()->query(<<<'SQL'
SELECT id, username, password_hash, enabled, created_at, updated_at
FROM users
ORDER BY username
SQL);

        if ($statement === false) {
            throw new RuntimeException('Could not read users from PostgreSQL.');
        }

        return array_map(
            fn (array $row): User => $this->mapRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function grantVirtualHost(string $username, string $virtualHost): void
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO user_virtual_hosts (user_id, virtual_host_id)
SELECT users.id, virtual_hosts.id
FROM users
CROSS JOIN virtual_hosts
WHERE users.username = :username
  AND virtual_hosts.name = :virtual_host
ON CONFLICT DO NOTHING
SQL);
        $statement->execute([
            'username' => $username,
            'virtual_host' => $virtualHost,
        ]);

        if ($statement->rowCount() === 0 && !$this->userVirtualHostGrantExists($username, $virtualHost)) {
            throw new RuntimeException(sprintf(
                'User "%s" does not have access to virtual host "%s". Grant the virtual host before clearing permissions.',
                $username,
                $virtualHost
            ));
        }
    }

    public function hasVirtualHostAccess(int $userId, string $virtualHost): bool
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT 1
FROM user_virtual_hosts
INNER JOIN virtual_hosts ON virtual_hosts.id = user_virtual_hosts.virtual_host_id
WHERE user_virtual_hosts.user_id = :user_id
  AND virtual_hosts.name = :virtual_host
SQL);
        $statement->execute([
            'user_id' => $userId,
            'virtual_host' => $virtualHost,
        ]);

        return $statement->fetchColumn() !== false;
    }

    /**
     * @return list<VirtualHost>
     */
    public function listVirtualHosts(int $userId): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT virtual_hosts.id,
       virtual_hosts.name,
       virtual_hosts.created_at
FROM user_virtual_hosts
INNER JOIN virtual_hosts ON virtual_hosts.id = user_virtual_hosts.virtual_host_id
WHERE user_virtual_hosts.user_id = :user_id
ORDER BY virtual_hosts.name
SQL);
        $statement->execute(['user_id' => $userId]);

        return array_map(
            fn (array $row): VirtualHost => $this->mapVirtualHostRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function setPermissions(
        string $username,
        string $virtualHost,
        string $configurePattern,
        string $writePattern,
        string $readPattern
    ): UserPermissions {
        $this->assertValidPattern($configurePattern, 'configure');
        $this->assertValidPattern($writePattern, 'write');
        $this->assertValidPattern($readPattern, 'read');

        $statement = $this->connection->pdo()->prepare(<<<'SQL'
INSERT INTO user_permissions (user_id, virtual_host_id, configure_pattern, write_pattern, read_pattern)
SELECT users.id, virtual_hosts.id, :configure_pattern, :write_pattern, :read_pattern
FROM users
INNER JOIN user_virtual_hosts ON user_virtual_hosts.user_id = users.id
INNER JOIN virtual_hosts ON virtual_hosts.id = user_virtual_hosts.virtual_host_id
WHERE users.username = :username
  AND virtual_hosts.name = :virtual_host
ON CONFLICT (user_id, virtual_host_id) DO UPDATE
SET configure_pattern = EXCLUDED.configure_pattern,
    write_pattern = EXCLUDED.write_pattern,
    read_pattern = EXCLUDED.read_pattern,
    updated_at = CURRENT_TIMESTAMP
RETURNING user_id, virtual_host_id, configure_pattern, write_pattern, read_pattern, created_at, updated_at
SQL);
        $statement->execute([
            'username' => $username,
            'virtual_host' => $virtualHost,
            'configure_pattern' => $configurePattern,
            'write_pattern' => $writePattern,
            'read_pattern' => $readPattern,
        ]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);
        if (!is_array($row)) {
            throw new RuntimeException(sprintf(
                'User "%s" does not have access to virtual host "%s". Grant the virtual host before setting permissions.',
                $username,
                $virtualHost
            ));
        }

        return $this->permissionsForUserVirtualHost((int) $row['user_id'], $virtualHost)
            ?? throw new RuntimeException('PostgreSQL did not return the saved user permissions.');
    }

    public function clearPermissions(string $username, string $virtualHost): void
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
DELETE FROM user_permissions
USING users, virtual_hosts, user_virtual_hosts
WHERE user_permissions.user_id = users.id
  AND user_permissions.virtual_host_id = virtual_hosts.id
  AND user_virtual_hosts.user_id = users.id
  AND user_virtual_hosts.virtual_host_id = virtual_hosts.id
  AND users.username = :username
  AND virtual_hosts.name = :virtual_host
SQL);
        $statement->execute([
            'username' => $username,
            'virtual_host' => $virtualHost,
        ]);

        if ($statement->rowCount() === 0 && ($this->findByUsername($username) === null || !$this->virtualHostExists($virtualHost))) {
            throw new RuntimeException(sprintf('User "%s" or virtual host "%s" was not found.', $username, $virtualHost));
        }
    }

    /**
     * @return list<UserPermissions>
     */
    public function permissionsForUsername(string $username): array
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT users.id AS user_id,
       users.username,
       virtual_hosts.id AS virtual_host_id,
       virtual_hosts.name AS virtual_host,
       user_permissions.configure_pattern,
       user_permissions.write_pattern,
       user_permissions.read_pattern,
       user_permissions.created_at,
       user_permissions.updated_at
FROM user_permissions
INNER JOIN users ON users.id = user_permissions.user_id
INNER JOIN virtual_hosts ON virtual_hosts.id = user_permissions.virtual_host_id
WHERE users.username = :username
ORDER BY virtual_hosts.name
SQL);
        $statement->execute(['username' => $username]);

        return array_map(
            fn (array $row): UserPermissions => $this->mapPermissionsRow($row),
            $statement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    public function permissionsForUserVirtualHost(int $userId, string $virtualHost): ?UserPermissions
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT users.id AS user_id,
       users.username,
       virtual_hosts.id AS virtual_host_id,
       virtual_hosts.name AS virtual_host,
       user_permissions.configure_pattern,
       user_permissions.write_pattern,
       user_permissions.read_pattern,
       user_permissions.created_at,
       user_permissions.updated_at
FROM user_permissions
INNER JOIN users ON users.id = user_permissions.user_id
INNER JOIN virtual_hosts ON virtual_hosts.id = user_permissions.virtual_host_id
WHERE users.id = :user_id
  AND virtual_hosts.name = :virtual_host
SQL);
        $statement->execute([
            'user_id' => $userId,
            'virtual_host' => $virtualHost,
        ]);

        $row = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($row) ? $this->mapPermissionsRow($row) : null;
    }

    private function virtualHostExists(string $virtualHost): bool
    {
        $statement = $this->connection->pdo()->prepare('SELECT 1 FROM virtual_hosts WHERE name = :name');
        $statement->execute(['name' => $virtualHost]);

        return $statement->fetchColumn() !== false;
    }

    private function userVirtualHostGrantExists(string $username, string $virtualHost): bool
    {
        $statement = $this->connection->pdo()->prepare(<<<'SQL'
SELECT 1
FROM user_virtual_hosts
INNER JOIN users ON users.id = user_virtual_hosts.user_id
INNER JOIN virtual_hosts ON virtual_hosts.id = user_virtual_hosts.virtual_host_id
WHERE users.username = :username
  AND virtual_hosts.name = :virtual_host
SQL);
        $statement->execute([
            'username' => $username,
            'virtual_host' => $virtualHost,
        ]);

        return $statement->fetchColumn() !== false;
    }

    private function passwordAlgorithm(): string
    {
        return defined('PASSWORD_ARGON2ID') ? PASSWORD_ARGON2ID : PASSWORD_DEFAULT;
    }

    private function assertValidPattern(string $pattern, string $permission): void
    {
        if (!ResourcePermissionMatcher::isValid($pattern)) {
            throw new RuntimeException(sprintf('Invalid %s permission expression.', $permission));
        }
    }

    /**
     * @param array{id: mixed, username: mixed, password_hash: mixed, enabled: mixed, created_at: mixed, updated_at: mixed} $row
     */
    private function mapRow(array $row): User
    {
        return new User(
            (int) $row['id'],
            (string) $row['username'],
            (string) $row['password_hash'],
            $row['enabled'] === true || $row['enabled'] === 1 || $row['enabled'] === '1' || $row['enabled'] === 't',
            new DateTimeImmutable((string) $row['created_at']),
            new DateTimeImmutable((string) $row['updated_at'])
        );
    }

    /**
     * @param array{id: mixed, name: mixed, created_at: mixed} $row
     */
    private function mapVirtualHostRow(array $row): VirtualHost
    {
        return new VirtualHost(
            (int) $row['id'],
            (string) $row['name'],
            new DateTimeImmutable((string) $row['created_at'])
        );
    }

    /**
     * @param array{user_id: mixed, username: mixed, virtual_host_id: mixed, virtual_host: mixed, configure_pattern: mixed, write_pattern: mixed, read_pattern: mixed, created_at: mixed, updated_at: mixed} $row
     */
    private function mapPermissionsRow(array $row): UserPermissions
    {
        return new UserPermissions(
            (int) $row['user_id'],
            (string) $row['username'],
            (int) $row['virtual_host_id'],
            (string) $row['virtual_host'],
            (string) $row['configure_pattern'],
            (string) $row['write_pattern'],
            (string) $row['read_pattern'],
            new DateTimeImmutable((string) $row['created_at']),
            new DateTimeImmutable((string) $row['updated_at'])
        );
    }
}
