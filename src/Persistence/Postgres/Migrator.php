<?php

declare(strict_types=1);

namespace FluxQ\Persistence\Postgres;

use PDO;
use RuntimeException;
use SplFileInfo;

final readonly class Migrator
{
    public function __construct(
        private Connection $connection,
        private string $migrationDirectory
    ) {
    }

    public function migrate(): MigrationResult
    {
        $this->ensureSchemaMigrationsTable();

        $appliedVersions = $this->appliedVersions();
        $applied = [];

        foreach ($this->migrationFiles() as $migrationFile) {
            $version = $this->versionFromFile($migrationFile);

            if (isset($appliedVersions[$version])) {
                continue;
            }

            $this->applyMigration($migrationFile, $version);
            $applied[] = $migrationFile->getBasename();
            $appliedVersions[$version] = true;
        }

        return new MigrationResult($applied);
    }

    /**
     * @return list<string>
     */
    public function migrationFilenames(): array
    {
        return self::discoverMigrationFilenames($this->migrationDirectory);
    }

    /**
     * @return list<string>
     */
    public static function discoverMigrationFilenames(string $migrationDirectory): array
    {
        return array_map(
            static fn (SplFileInfo $file): string => $file->getBasename(),
            self::migrationFilesIn($migrationDirectory)
        );
    }

    private function ensureSchemaMigrationsTable(): void
    {
        $this->connection->pdo()->exec(<<<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
)
SQL);

        $this->connection->pdo()->exec(
            'CREATE INDEX IF NOT EXISTS schema_migrations_applied_at_idx ON schema_migrations (applied_at)'
        );
    }

    public function pendingMigrationCount(): ?int
    {
        if (!$this->hasSchemaMigrationsTable()) {
            return null;
        }

        $appliedVersions = $this->appliedVersions();
        $pending = 0;

        foreach ($this->migrationFiles() as $migrationFile) {
            if (!isset($appliedVersions[$this->versionFromFile($migrationFile)])) {
                $pending++;
            }
        }

        return $pending;
    }

    private function hasSchemaMigrationsTable(): bool
    {
        $statement = $this->connection->pdo()->prepare('SELECT to_regclass(:table)');
        $statement->execute(['table' => 'schema_migrations']);

        return $statement->fetchColumn() === 'schema_migrations';
    }

    /**
     * @return array<string, true>
     */
    private function appliedVersions(): array
    {
        $statement = $this->connection->pdo()->query('SELECT version FROM schema_migrations');

        if ($statement === false) {
            throw new RuntimeException('Could not read applied PostgreSQL migrations.');
        }

        $versions = [];

        foreach ($statement->fetchAll(PDO::FETCH_COLUMN) as $version) {
            if (is_string($version)) {
                $versions[$version] = true;
            }
        }

        return $versions;
    }

    /**
     * @return list<SplFileInfo>
     */
    private function migrationFiles(): array
    {
        return self::migrationFilesIn($this->migrationDirectory);
    }

    /**
     * @return list<SplFileInfo>
     */
    private static function migrationFilesIn(string $migrationDirectory): array
    {
        $directory = realpath($migrationDirectory);

        if ($directory === false || !is_dir($directory)) {
            throw new RuntimeException(sprintf('Migration directory does not exist: %s', $migrationDirectory));
        }

        $files = glob($directory . DIRECTORY_SEPARATOR . '*.sql');

        if ($files === false) {
            throw new RuntimeException(sprintf('Could not read migration directory: %s', $directory));
        }

        sort($files, SORT_STRING);

        return array_map(
            static fn (string $file): SplFileInfo => new SplFileInfo($file),
            $files
        );
    }

    private function applyMigration(SplFileInfo $migrationFile, string $version): void
    {
        $sql = file_get_contents($migrationFile->getPathname());

        if ($sql === false) {
            throw new RuntimeException(sprintf('Could not read migration file: %s', $migrationFile->getPathname()));
        }

        try {
            $this->connection->transaction(function (PDO $pdo) use ($sql, $version): void {
                $pdo->exec($sql);

                $statement = $pdo->prepare(
                    'INSERT INTO schema_migrations (version) VALUES (:version) ON CONFLICT (version) DO NOTHING'
                );
                $statement->execute(['version' => $version]);
            });
        } catch (\Throwable $exception) {

            throw new MigrationFailure($migrationFile->getBasename(), $exception);
        }
    }

    private function versionFromFile(SplFileInfo $migrationFile): string
    {
        return pathinfo($migrationFile->getBasename(), PATHINFO_FILENAME);
    }
}
