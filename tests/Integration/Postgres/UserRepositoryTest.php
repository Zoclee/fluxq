<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Postgres;

use FluxQ\Broker\Authenticator;
use FluxQ\Broker\AuthorizationPermission;
use FluxQ\Broker\Authorizer;
use FluxQ\Console\Commands\UserClearPermissionsCommand;
use FluxQ\Console\Commands\UserGrantVhostCommand;
use FluxQ\Console\Commands\UserListCommand;
use FluxQ\Console\Commands\UserListPermissionsCommand;
use FluxQ\Console\Commands\UserListVhostsCommand;
use FluxQ\Console\Commands\UserSetPermissionsCommand;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\UserRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class UserRepositoryTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private UserRepository $users;

    #[Before]
    public function setUpRepository(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for PostgreSQL user integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');
        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run PostgreSQL user integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();
        $this->assertSafeTestDatabase();
        $this->resetSchema();
        (new Migrator($this->connection, dirname(__DIR__, 3) . '/database/migrations'))->migrate();
        $this->users = new UserRepository($this->connection);
    }

    public function testUserCreationRequiresUniqueUsernameAndStoresHashedPassword(): void
    {
        $user = $this->users->create('alice', 'correct horse battery staple');

        self::assertSame('alice', $user->username);
        self::assertNotSame('correct horse battery staple', $user->passwordHash);
        self::assertTrue(password_verify('correct horse battery staple', $user->passwordHash));

        $this->expectException(PDOException::class);
        $this->users->create('alice', 'different');
    }

    public function testAuthenticatorAcceptsCorrectPasswordAndRejectsIncorrectOrDisabledUsers(): void
    {
        $this->users->create('alice', 'secret');
        $this->users->create('disabled', 'secret');
        $this->pdo->exec("UPDATE users SET enabled = false WHERE username = 'disabled'");
        $authenticator = new Authenticator($this->users);

        self::assertTrue($authenticator->authenticate('alice', 'secret')->authenticated);
        self::assertFalse($authenticator->authenticate('alice', 'wrong')->authenticated);
        self::assertFalse($authenticator->authenticate('disabled', 'secret')->authenticated);
    }

    public function testVirtualHostGrantAllowsOnlyGrantedKnownVirtualHosts(): void
    {
        $this->users->create('alice', 'secret');
        (new VirtualHostRepository($this->connection))->create('tenant-a');
        $this->users->grantVirtualHost('alice', '/');
        $authenticator = new Authenticator($this->users);
        $result = $authenticator->authenticate('alice', 'secret');

        self::assertTrue($result->authenticated);
        self::assertNotNull($result->user);
        self::assertTrue($authenticator->canAccessVirtualHost($result->user, '/'));
        self::assertFalse($authenticator->canAccessVirtualHost($result->user, 'tenant-a'));
        self::assertFalse($authenticator->canAccessVirtualHost($result->user, 'missing'));
    }

    public function testListVirtualHostsReturnsOneGrant(): void
    {
        $user = $this->users->create('alice', 'secret');
        $this->users->grantVirtualHost('alice', '/');

        self::assertSame(
            ['/'],
            array_map(static fn ($virtualHost): string => $virtualHost->name, $this->users->listVirtualHosts($user->id))
        );
    }

    public function testListVirtualHostsReturnsMultipleGrantsInNameOrder(): void
    {
        $user = $this->users->create('alice', 'secret');
        $virtualHosts = new VirtualHostRepository($this->connection);
        $virtualHosts->create('/z');
        $virtualHosts->create('/a');

        $this->users->grantVirtualHost('alice', '/z');
        $this->users->grantVirtualHost('alice', '/');
        $this->users->grantVirtualHost('alice', '/a');

        self::assertSame(
            ['/', '/a', '/z'],
            array_map(static fn ($virtualHost): string => $virtualHost->name, $this->users->listVirtualHosts($user->id))
        );

        [$exitCode, $output] = $this->runArgumentCommand(new UserListVhostsCommand($this->connection), ['alice']);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('3 virtual hosts.', $output);
        self::assertLessThan(strpos($output, '/a'), strpos($output, '/'));
        self::assertLessThan(strpos($output, '/z'), strpos($output, '/a'));
    }

    public function testListVhostsCommandHandlesNoGrantsAndUnknownUsersWithCorrectExitCodes(): void
    {
        $this->users->create('alice', 'secret');
        $command = new UserListVhostsCommand($this->connection);

        [$emptyExitCode, $emptyOutput] = $this->runArgumentCommand($command, ['alice']);
        self::assertSame(0, $emptyExitCode);
        self::assertSame("No virtual-host grants found for user \"alice\".\n", $emptyOutput);

        [$missingExitCode, $missingOutput] = $this->runArgumentCommand($command, ['missing']);
        self::assertSame(1, $missingExitCode);
        self::assertSame("ERROR: User \"missing\" was not found.\n", $missingOutput);

        [$usageExitCode, $usageOutput] = $this->runArgumentCommand($command, ['alice', 'extra']);
        self::assertSame(1, $usageExitCode);
        self::assertSame("Usage: fluxq user:list-vhosts <username>\n", $usageOutput);
    }

    public function testGrantVhostThenListVhostsShowsGrantWithoutPermissionRows(): void
    {
        $this->users->create('alice', 'secret');
        (new VirtualHostRepository($this->connection))->create('/test');

        [$grantExitCode] = $this->runArgumentCommand(new UserGrantVhostCommand($this->connection), ['alice', '/test']);
        self::assertSame(0, $grantExitCode);

        [$vhostsExitCode, $vhostsOutput] = $this->runArgumentCommand(new UserListVhostsCommand($this->connection), ['alice']);
        self::assertSame(0, $vhostsExitCode);
        self::assertStringContainsString('Virtual Hosts for user "alice"', $vhostsOutput);
        self::assertStringContainsString('/test', $vhostsOutput);
        self::assertStringContainsString('1 virtual host.', $vhostsOutput);

        [$permissionsExitCode, $permissionsOutput] = $this->runArgumentCommand(new UserListPermissionsCommand($this->connection), ['alice']);
        self::assertSame(0, $permissionsExitCode);
        self::assertSame("No permissions found for user \"alice\".\n", $permissionsOutput);
    }

    public function testGrantCommandGrantsVhostAndListCommandDoesNotExposeSecrets(): void
    {
        $user = $this->users->create('alice', 'secret');

        $grantOutput = fopen('php://temp', 'w+');
        self::assertIsResource($grantOutput);
        self::assertSame(0, (new UserGrantVhostCommand($this->connection))->run(['alice', '/'], $grantOutput));

        $result = (new Authenticator($this->users))->authenticate('alice', 'secret');
        self::assertNotNull($result->user);
        self::assertTrue((new Authenticator($this->users))->canAccessVirtualHost($result->user, '/'));

        $listOutput = fopen('php://temp', 'w+');
        self::assertIsResource($listOutput);
        self::assertSame(0, (new UserListCommand($this->connection))->run($listOutput));
        rewind($listOutput);
        $output = stream_get_contents($listOutput);
        self::assertIsString($output);
        self::assertStringContainsString('alice', $output);
        self::assertStringNotContainsString('secret', $output);
        self::assertStringNotContainsString($user->passwordHash, $output);
    }

    public function testPermissionsPersistAndAuthorizeRegexResources(): void
    {
        $this->users->create('alice', 'secret');
        $this->users->grantVirtualHost('alice', '/');
        $permissions = $this->users->setPermissions('alice', '/', '^orders\\.', '^orders\\.direct$', '^orders$');
        $result = (new Authenticator($this->users))->authenticate('alice', 'secret');
        self::assertNotNull($result->user);
        $authorizer = new Authorizer($this->users);

        self::assertSame('^orders\\.', $permissions->configurePattern);
        self::assertTrue($authorizer->authorize($result->user, '/', AuthorizationPermission::Configure, 'orders.queue')->allowed);
        self::assertFalse($authorizer->authorize($result->user, '/', AuthorizationPermission::Configure, 'billing.queue')->allowed);
        self::assertTrue($authorizer->authorize($result->user, '/', AuthorizationPermission::Write, 'orders.direct')->allowed);
        self::assertFalse($authorizer->authorize($result->user, '/', AuthorizationPermission::Write, 'orders.topic')->allowed);
        self::assertTrue($authorizer->authorize($result->user, '/', AuthorizationPermission::Read, 'orders')->allowed);
        self::assertFalse($authorizer->authorize($result->user, '/', AuthorizationPermission::Read, 'orders.dead')->allowed);
    }

    public function testMissingPermissionsDenyAndDifferentUsersOrVhostsRemainIsolated(): void
    {
        $virtualHosts = new VirtualHostRepository($this->connection);
        $virtualHosts->create('tenant-a');
        $this->users->create('alice', 'secret');
        $this->users->create('bob', 'secret');
        $this->users->grantVirtualHost('alice', '/');
        $this->users->grantVirtualHost('alice', 'tenant-a');
        $this->users->grantVirtualHost('bob', '/');
        $this->users->setPermissions('alice', '/', '^alice$', '^alice$', '^alice$');
        $this->users->setPermissions('alice', 'tenant-a', '^tenant$', '^tenant$', '^tenant$');
        $this->users->setPermissions('bob', '/', '^bob$', '^bob$', '^bob$');
        $authorizer = new Authorizer($this->users);
        $alice = (new Authenticator($this->users))->authenticate('alice', 'secret')->user;
        $bob = (new Authenticator($this->users))->authenticate('bob', 'secret')->user;
        self::assertNotNull($alice);
        self::assertNotNull($bob);

        self::assertTrue($authorizer->authorize($alice, '/', AuthorizationPermission::Read, 'alice')->allowed);
        self::assertFalse($authorizer->authorize($alice, '/', AuthorizationPermission::Read, 'tenant')->allowed);
        self::assertTrue($authorizer->authorize($alice, 'tenant-a', AuthorizationPermission::Read, 'tenant')->allowed);
        self::assertFalse($authorizer->authorize($bob, '/', AuthorizationPermission::Read, 'alice')->allowed);
        self::assertFalse($authorizer->authorize($bob, 'tenant-a', AuthorizationPermission::Read, 'tenant')->allowed);
    }

    public function testInvalidPermissionExpressionIsRejected(): void
    {
        $this->users->create('alice', 'secret');
        $this->users->grantVirtualHost('alice', '/');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Invalid configure permission expression.');

        $this->users->setPermissions('alice', '/', '[', '.*', '.*');
    }

    public function testPermissionsRequireExistingVirtualHostGrant(): void
    {
        $this->users->create('alice', 'secret');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Grant the virtual host before setting permissions.');

        $this->users->setPermissions('alice', '/', '.*', '.*', '.*');
    }

    public function testPermissionCommandsSetListAndClearPermissions(): void
    {
        $this->users->create('alice', 'secret');
        $this->users->grantVirtualHost('alice', '/');
        $result = (new Authenticator($this->users))->authenticate('alice', 'secret');
        self::assertNotNull($result->user);
        $authorizer = new Authorizer($this->users);

        $setOutput = fopen('php://temp', 'w+');
        self::assertIsResource($setOutput);
        self::assertSame(0, (new UserSetPermissionsCommand($this->connection))->run(['alice', '/', '^cfg$', '^wr$', '^rd$'], $setOutput));
        self::assertTrue($authorizer->authorize($result->user, '/', AuthorizationPermission::Configure, 'cfg')->allowed);
        self::assertFalse($authorizer->authorize($result->user, '/', AuthorizationPermission::Configure, 'other')->allowed);

        $listOutput = fopen('php://temp', 'w+');
        self::assertIsResource($listOutput);
        self::assertSame(0, (new UserListPermissionsCommand($this->connection))->run(['alice'], $listOutput));
        rewind($listOutput);
        $output = stream_get_contents($listOutput);
        self::assertIsString($output);
        self::assertStringContainsString('^cfg$', $output);
        self::assertStringContainsString('^wr$', $output);
        self::assertStringContainsString('^rd$', $output);

        $clearOutput = fopen('php://temp', 'w+');
        self::assertIsResource($clearOutput);
        self::assertSame(0, (new UserClearPermissionsCommand($this->connection))->run(['alice', '/'], $clearOutput));
        self::assertFalse($authorizer->authorize($result->user, '/', AuthorizationPermission::Configure, 'cfg')->allowed);
    }

    /**
     * @param list<string> $arguments
     * @return array{0: int, 1: string}
     */
    private function runArgumentCommand(object $command, array $arguments): array
    {
        $stream = fopen('php://memory', 'w+');
        self::assertIsResource($stream);

        $exitCode = $command->run($arguments, $stream);

        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);

        return [$exitCode, $output];
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function assertSafeTestDatabase(): void
    {
        $database = (string) $this->pdo->query('SELECT current_database()')->fetchColumn();

        if (!str_contains(strtolower($database), 'test')) {
            self::markTestSkipped(sprintf(
                'Refusing to reset PostgreSQL database "%s"; FLUXQ_TEST_DATABASE_URL must point to a test database.',
                $database
            ));
        }
    }
}
