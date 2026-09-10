<?php

declare(strict_types=1);

namespace FluxQ\Tests\Fixtures;

use RuntimeException;

final readonly class TlsCertificate
{
    /**
     * @return array{cert: string, key: string, dir: string}
     */
    public static function create(): array
    {
        $directory = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'fluxq_tls_' . bin2hex(random_bytes(8));
        if (!mkdir($directory) && !is_dir($directory)) {
            throw new RuntimeException(sprintf('Could not create TLS fixture directory "%s".', $directory));
        }

        $certPath = $directory . DIRECTORY_SEPARATOR . 'server.crt';
        $keyPath = $directory . DIRECTORY_SEPARATOR . 'server.key';

        if (extension_loaded('openssl') && self::createWithPhpOpenSsl($certPath, $keyPath)) {
            return ['cert' => $certPath, 'key' => $keyPath, 'dir' => $directory];
        }

        self::createWithOpenSslCli($certPath, $keyPath);

        return ['cert' => $certPath, 'key' => $keyPath, 'dir' => $directory];
    }

    private static function createWithPhpOpenSsl(string $certPath, string $keyPath): bool
    {
        $key = openssl_pkey_new([
            'private_key_bits' => 2048,
            'private_key_type' => OPENSSL_KEYTYPE_RSA,
        ]);
        if ($key === false) {
            return false;
        }

        $csr = openssl_csr_new(['commonName' => 'localhost'], $key);
        if ($csr === false) {
            return false;
        }

        $certificate = openssl_csr_sign($csr, null, $key, 1);
        if ($certificate === false) {
            return false;
        }

        $certPem = '';
        $keyPem = '';
        if (!openssl_x509_export($certificate, $certPem) || !openssl_pkey_export($key, $keyPem)) {
            return false;
        }

        file_put_contents($certPath, $certPem);
        file_put_contents($keyPath, $keyPem);

        return true;
    }

    private static function createWithOpenSslCli(string $certPath, string $keyPath): void
    {
        $process = proc_open(
            [
                'openssl',
                'req',
                '-x509',
                '-newkey',
                'rsa:2048',
                '-nodes',
                '-keyout',
                $keyPath,
                '-out',
                $certPath,
                '-days',
                '1',
                '-subj',
                '/CN=localhost',
            ],
            [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            $pipes
        );

        if (!is_resource($process)) {
            throw new RuntimeException('The openssl command is required to create TLS test certificates.');
        }

        fclose($pipes[0]);
        stream_get_contents($pipes[1]);
        fclose($pipes[1]);
        $error = stream_get_contents($pipes[2]);
        fclose($pipes[2]);
        $exitCode = proc_close($process);

        if ($exitCode !== 0 || !is_file($certPath) || !is_file($keyPath)) {
            throw new RuntimeException('Could not create TLS fixture certificate: ' . trim($error));
        }
    }
}
