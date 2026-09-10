<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Console;

use FluxQ\Console\Application;
use PHPUnit\Framework\TestCase;

final class ApplicationTest extends TestCase
{
    public function testItPrintsHelpWhenNoCommandIsProvided(): void
    {
        [$exitCode, $output] = $this->runApplication(['fluxq']);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('FluxQ ' . Application::VERSION, $output);
        self::assertStringContainsString('Usage:', $output);
        self::assertStringContainsString('db:status', $output);
        self::assertStringContainsString('migrate', $output);
        self::assertStringContainsString('health', $output);
        self::assertStringContainsString('readiness', $output);
        self::assertStringContainsString('server:start', $output);
        self::assertStringContainsString('user:list-vhosts <username>', $output);
        self::assertStringContainsString('vhost:create <name>', $output);
        self::assertStringContainsString('vhost:list', $output);
        self::assertStringContainsString('queue:list', $output);
        self::assertStringContainsString('queue:show <queue>', $output);
        self::assertStringContainsString('binding:list', $output);
        self::assertStringContainsString('subscription:list', $output);
        self::assertStringContainsString('message:peek <queue>', $output);
    }

    public function testItPrintsTheVersion(): void
    {
        [$exitCode, $output] = $this->runApplication(['fluxq', '--version']);

        self::assertSame(0, $exitCode);
        self::assertSame("FluxQ " . Application::VERSION . "\n", $output);
    }

    public function testRootCliEntryPointExists(): void
    {
        $projectRoot = dirname(__DIR__, 3);

        self::assertFileExists($projectRoot . '/fluxq');
        self::assertFileDoesNotExist($projectRoot . '/bin' . DIRECTORY_SEPARATOR . 'fluxq');
    }

    public function testRootCliEntryPointUsesRepositoryAutoloaderFirst(): void
    {
        $layout = $this->createCliBootstrapLayout();

        mkdir($layout . '/vendor', 0777, true);
        file_put_contents($layout . '/vendor/autoload.php', $this->fakeAutoloader('repository', 17));

        mkdir($layout . '/parent', 0777, true);
        file_put_contents($layout . '/parent/autoload.php', $this->fakeAutoloader('parent', 19));

        [$exitCode, $stdout, $stderr] = $this->runCliBootstrap($layout . '/fluxq');

        self::assertSame(17, $exitCode);
        self::assertSame("repository\n", $stdout);
        self::assertSame('', $stderr);
    }

    public function testRootCliEntryPointFallsBackToComposerDependencyAutoloader(): void
    {
        $layout = $this->createCliBootstrapLayout('vendor/zoclee/fluxq');

        file_put_contents(dirname($layout, 2) . '/autoload.php', $this->fakeAutoloader('composer', 23));

        [$exitCode, $stdout, $stderr] = $this->runCliBootstrap($layout . '/fluxq');

        self::assertSame(23, $exitCode);
        self::assertSame("composer\n", $stdout);
        self::assertSame('', $stderr);
    }

    public function testRootCliEntryPointReportsMissingAutoloader(): void
    {
        $layout = $this->createCliBootstrapLayout();

        [$exitCode, $stdout, $stderr] = $this->runCliBootstrap($layout . '/fluxq');

        self::assertSame(1, $exitCode);
        self::assertSame('', $stdout);
        self::assertStringContainsString("FluxQ could not find Composer's autoloader.", $stderr);
    }

    /**
     * @param list<string> $argv
     * @return array{0: int, 1: string}
     */
    private function runApplication(array $argv): array
    {
        $stream = fopen('php://memory', 'w+');

        self::assertIsResource($stream);

        $exitCode = (new Application())->run($argv, $stream);

        rewind($stream);
        $output = stream_get_contents($stream);
        fclose($stream);

        self::assertIsString($output);

        return [$exitCode, $output];
    }

    private function createCliBootstrapLayout(string $relativePath = ''): string
    {
        $root = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'fluxq-cli-bootstrap-' . bin2hex(random_bytes(8));
        $layout = $relativePath === '' ? $root : $root . DIRECTORY_SEPARATOR . str_replace('/', DIRECTORY_SEPARATOR, $relativePath);

        mkdir($layout, 0777, true);
        copy(dirname(__DIR__, 3) . '/fluxq', $layout . '/fluxq');

        return $layout;
    }

    /**
     * @return array{0: int, 1: string, 2: string}
     */
    private function runCliBootstrap(string $script): array
    {
        $process = proc_open(
            [PHP_BINARY, $script],
            [
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            $pipes,
        );

        self::assertIsResource($process);

        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);

        fclose($pipes[1]);
        fclose($pipes[2]);

        $exitCode = proc_close($process);

        self::assertIsString($stdout);
        self::assertIsString($stderr);

        return [$exitCode, $stdout, $stderr];
    }

    private function fakeAutoloader(string $message, int $exitCode): string
    {
        return <<<PHP
<?php

declare(strict_types=1);

namespace FluxQ\Console;

final class Application
{
    public function run(array \$argv): int
    {
        fwrite(STDOUT, "{$message}\n");

        return {$exitCode};
    }
}

PHP;
    }
}
