<?php

declare(strict_types=1);

namespace FluxQ\Protocol\Amqp;

use RuntimeException;

final readonly class AmqpTlsConfig
{
    public function __construct(
        public string $certificate,
        public string $privateKey,
        public ?string $certificateAuthority = null
    ) {
        $this->assertReadableFile($this->certificate, 'AMQP TLS certificate');
        $this->assertReadableFile($this->privateKey, 'AMQP TLS private key');

        if ($this->certificateAuthority !== null && $this->certificateAuthority !== '') {
            $this->assertReadableFile($this->certificateAuthority, 'AMQP TLS certificate authority');
        }
    }

    /**
     * @return array<string, mixed>
     */
    public function streamContextOptions(): array
    {
        $options = [
            'local_cert' => $this->certificate,
            'local_pk' => $this->privateKey,
            'verify_peer' => false,
            'verify_peer_name' => false,
            'allow_self_signed' => true,
            'disable_compression' => true,
            'crypto_method' => self::serverCryptoMethod(),
        ];

        if ($this->certificateAuthority !== null && $this->certificateAuthority !== '') {
            $options['cafile'] = $this->certificateAuthority;
        }

        return $options;
    }

    public static function serverCryptoMethod(): int
    {
        $method = 0;

        if (defined('STREAM_CRYPTO_METHOD_TLSv1_2_SERVER')) {
            $method |= STREAM_CRYPTO_METHOD_TLSv1_2_SERVER;
        }

        if (defined('STREAM_CRYPTO_METHOD_TLSv1_3_SERVER')) {
            $method |= STREAM_CRYPTO_METHOD_TLSv1_3_SERVER;
        }

        return $method !== 0 ? $method : STREAM_CRYPTO_METHOD_TLS_SERVER;
    }

    private function assertReadableFile(string $path, string $label): void
    {
        if ($path === '') {
            throw new RuntimeException(sprintf('%s path is required.', $label));
        }

        if (!is_file($path) || !is_readable($path)) {
            throw new RuntimeException(sprintf('%s file "%s" is not readable.', $label, $path));
        }
    }
}
