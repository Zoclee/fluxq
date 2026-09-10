<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Persistence\Postgres;

use DateTimeImmutable;
use FluxQ\Persistence\Postgres\Migrator;
use PHPUnit\Framework\TestCase;

final class MigrationFilenameTest extends TestCase
{
    public function testMigrationFilenamesUseUniqueSortableTimestamps(): void
    {
        $migrationFilenames = Migrator::discoverMigrationFilenames(dirname(__DIR__, 4) . '/database/migrations');

        $timestamps = [];

        foreach ($migrationFilenames as $filename) {
            self::assertMatchesRegularExpression(
                '/^(?<timestamp>\d{8}_\d{6})_[a-z0-9_]+\.sql$/',
                $filename
            );

            $timestamp = substr($filename, 0, 15);
            self::assertNotContains($timestamp, $timestamps);
            $timestamps[] = $timestamp;

            $date = DateTimeImmutable::createFromFormat('Ymd_His', $timestamp);
            self::assertInstanceOf(DateTimeImmutable::class, $date);
            self::assertSame($timestamp, $date->format('Ymd_His'));
        }

        $sortedTimestamps = $timestamps;
        sort($sortedTimestamps);

        self::assertSame($sortedTimestamps, $timestamps);
    }

    public function testMigrationDiscoveryUsesLexicalFilenameOrder(): void
    {
        self::assertSame(
            [
                '20260820_120000_create_schema_migrations.sql',
                '20260820_120001_create_virtual_hosts.sql',
                '20260820_120002_create_destinations.sql',
                '20260820_120003_create_bindings.sql',
                '20260820_120004_create_messages.sql',
                '20260820_120005_create_message_routes.sql',
                '20260820_120006_create_subscriptions.sql',
                '20260820_120007_create_deliveries.sql',
                '20260820_120008_enforce_binding_destination_virtual_host.sql',
                '20260820_120009_enforce_delivery_route_subscription_destination.sql',
                '20260820_120010_create_routing_sources.sql',
                '20260820_120011_create_users.sql',
                '20260820_120012_create_user_permissions.sql',
                '20260820_120013_allow_fanout_routing_sources.sql',
                '20260820_120014_allow_topic_routing_sources.sql',
                '20260820_120015_add_message_metadata.sql',
            ],
            Migrator::discoverMigrationFilenames(dirname(__DIR__, 4) . '/database/migrations')
        );
    }
}
