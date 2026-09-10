<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Protocol\Amqp;

use FluxQ\Broker\AuthenticatedUser;
use FluxQ\Broker\AuthenticationResult;
use FluxQ\Broker\AuthenticationService;
use FluxQ\Broker\ResourceLimits;
use FluxQ\Protocol\Amqp\AmqpConnection;
use FluxQ\Protocol\Amqp\AmqpListener;
use FluxQ\Protocol\Amqp\AmqpTlsConfig;
use FluxQ\Protocol\Amqp\Frame;
use FluxQ\Protocol\Amqp\FrameCodec;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Tests\Fixtures\TlsCertificate;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class AmqpListenerTest extends TestCase
{
    public function testListenerCompletesConnectionHandshakeOverTcp(): void
    {
        $runtimeConnections = new ConnectionRegistry();
        $listener = new AmqpListener($runtimeConnections, '127.0.0.1', 0, authenticator: $this->authenticator());
        $listener->start();

        try {
            $client = $this->connect($listener);
            $this->connectAmqp($listener, $client, 60);
            self::assertSame(1, $runtimeConnections->count());
            fclose($client);
        } finally {
            $listener->stop();
        }

        self::assertSame(0, $runtimeConnections->count());
    }

    public function testListenerRejectsInvalidProtocolHeaderCleanly(): void
    {
        $listener = new AmqpListener(new ConnectionRegistry(), '127.0.0.1', 0);
        $listener->start();

        try {
            $client = $this->connect($listener);
            fwrite($client, "AMQP\x00\x00\x09\x00");

            for ($attempt = 0; $attempt < 20; $attempt++) {
                $listener->tick();
                if (feof($client)) {
                    break;
                }
                usleep(1000);
            }

            self::assertTrue(feof($client));
            fclose($client);
        } finally {
            $listener->stop();
        }
    }

    public function testTlsListenerStartsAndCompletesConnectionHandshakeOverTcp(): void
    {
        $tls = $this->tlsConfig();
        $runtimeConnections = new ConnectionRegistry();
        $listener = new AmqpListener($runtimeConnections, '127.0.0.1', 0, authenticator: $this->authenticator(), tls: $tls);
        $listener->start();

        try {
            $client = $this->connectTls($listener);
            $this->connectAmqp($listener, $client, 60);
            self::assertSame(1, $runtimeConnections->count());
            fclose($client);
        } finally {
            $listener->stop();
        }

        self::assertSame(0, $runtimeConnections->count());
    }

    public function testConnectionLimitRejectsAdditionalPlaintextClientsAndRecoversCapacity(): void
    {
        $runtimeConnections = new ConnectionRegistry();
        $listener = new AmqpListener(
            $runtimeConnections,
            '127.0.0.1',
            0,
            authenticator: $this->authenticator(),
            limits: new ResourceLimits(maxConnections: 1)
        );
        $listener->start();

        try {
            $first = $this->connect($listener);
            $this->connectAmqp($listener, $first, 60);
            self::assertSame(1, $runtimeConnections->count());

            $second = $this->connect($listener);
            $this->awaitEof($listener, $second);
            fclose($second);
            self::assertSame(1, $runtimeConnections->count());

            fclose($first);
            $listener->tick();
            self::assertSame(0, $runtimeConnections->count());

            $third = $this->connect($listener);
            $this->connectAmqp($listener, $third, 60);
            self::assertSame(1, $runtimeConnections->count());
            fclose($third);
        } finally {
            $listener->stop();
        }
    }

    public function testDrainStopsAcceptingNewPlaintextConnections(): void
    {
        $listener = new AmqpListener(new ConnectionRegistry(), '127.0.0.1', 0, authenticator: $this->authenticator());
        $listener->start();
        $port = $listener->port();
        $listener->beginDrain();

        try {
            $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $port), $errorCode, $errorMessage, 0.1);
            if (is_resource($client)) {
                fclose($client);
            }

            self::assertFalse($client);
            self::assertSame(0, $listener->connectionCount());
        } finally {
            $listener->stop();
        }
    }

    public function testConnectionLimitRejectsAdditionalTlsClients(): void
    {
        $runtimeConnections = new ConnectionRegistry();
        $listener = new AmqpListener(
            $runtimeConnections,
            '127.0.0.1',
            0,
            authenticator: $this->authenticator(),
            tls: $this->tlsConfig(),
            limits: new ResourceLimits(maxConnections: 1)
        );
        $listener->start();

        try {
            $first = $this->connectTls($listener);
            $this->connectAmqp($listener, $first, 60);
            self::assertSame(1, $runtimeConnections->count());

            $second = $this->connect($listener);
            $this->awaitEof($listener, $second);
            fclose($second);
            fclose($first);
        } finally {
            $listener->stop();
        }
    }

    public function testChannelLimitClosesRejectedChannelAndKeepsConnectionHealthy(): void
    {
        $listener = new AmqpListener(
            new ConnectionRegistry(),
            '127.0.0.1',
            0,
            authenticator: $this->authenticator(),
            limits: new ResourceLimits(maxChannelsPerConnection: 1)
        );
        $listener->start();

        try {
            $client = $this->connect($listener);
            $codec = new FrameCodec();
            $this->connectAmqp($listener, $client, 60);

            fwrite($client, $codec->encode(Frame::methodFrame(1, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            $close = $this->readFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(506, $this->replyCode($close));
            self::assertSame(1, $listener->connectionCount());
            fclose($client);
        } finally {
            $listener->stop();
        }
    }

    public function testInvalidTlsCertificatePathFailsClearly(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('AMQP TLS certificate file');

        new AmqpTlsConfig(__DIR__ . '/missing.crt', __FILE__);
    }

    public function testInvalidTlsKeyPathFailsClearly(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('AMQP TLS private key file');

        new AmqpTlsConfig(__FILE__, __DIR__ . '/missing.key');
    }

    public function testMalformedTlsClientDoesNotCrashListener(): void
    {
        $listener = new AmqpListener(new ConnectionRegistry(), '127.0.0.1', 0, authenticator: $this->authenticator(), tls: $this->tlsConfig());
        $listener->start();

        try {
            $client = $this->connect($listener);
            fwrite($client, "not tls");

            for ($attempt = 0; $attempt < 20; $attempt++) {
                $listener->tick();
                if (feof($client)) {
                    break;
                }
                usleep(1000);
            }

            self::assertTrue(feof($client));
            fclose($client);
            $listener->tick();
            self::assertSame(0, $listener->connectionCount());
        } finally {
            $listener->stop();
        }
    }

    public function testStopClosesTlsClients(): void
    {
        $listener = new AmqpListener(new ConnectionRegistry(), '127.0.0.1', 0, authenticator: $this->authenticator(), tls: $this->tlsConfig());
        $listener->start();

        $client = $this->connectTls($listener);
        $this->connectAmqp($listener, $client, 60);
        self::assertSame(1, $listener->connectionCount());

        $listener->stop();

        for ($attempt = 0; $attempt < 20 && !feof($client); $attempt++) {
            fread($client, 8192);
            usleep(1000);
        }

        self::assertTrue(feof($client));
        fclose($client);
    }

    public function testDrainThenStopClosesTlsClients(): void
    {
        $listener = new AmqpListener(new ConnectionRegistry(), '127.0.0.1', 0, authenticator: $this->authenticator(), tls: $this->tlsConfig());
        $listener->start();
        $client = $this->connectTls($listener);
        $this->connectAmqp($listener, $client, 60);

        self::assertSame(1, $listener->connectionCount());
        $listener->beginDrain();
        $listener->stop();

        for ($attempt = 0; $attempt < 20 && !feof($client); $attempt++) {
            fread($client, 8192);
            usleep(1000);
        }

        self::assertTrue(feof($client));
        fclose($client);
    }

    public function testIdleConnectionSendsHeartbeatFrame(): void
    {
        $now = 0;
        $listener = new AmqpListener(
            new ConnectionRegistry(),
            '127.0.0.1',
            0,
            authenticator: $this->authenticator(),
            heartbeatInterval: 5,
            clock: static function () use (&$now): int {
                return $now * 1_000_000_000;
            }
        );
        $listener->start();

        try {
            $client = $this->connect($listener);
            $this->connectAmqp($listener, $client, 5);

            $now = 4;
            $listener->tick();
            self::assertSame('', fread($client, 8192));

            $now = 5;
            $frame = $this->readFrame($listener, $client);

            self::assertSame(Frame::TYPE_HEARTBEAT, $frame->type);
            self::assertSame(0, $frame->channel);
            self::assertSame('', $frame->payload);
            fclose($client);
        } finally {
            $listener->stop();
        }
    }

    public function testDrainingConnectionStillSendsHeartbeatFrame(): void
    {
        $now = 0;
        $listener = new AmqpListener(
            new ConnectionRegistry(),
            '127.0.0.1',
            0,
            authenticator: $this->authenticator(),
            heartbeatInterval: 5,
            clock: static function () use (&$now): int {
                return $now * 1_000_000_000;
            }
        );
        $listener->start();

        try {
            $client = $this->connect($listener);
            $this->connectAmqp($listener, $client, 5);
            $listener->beginDrain();
            $now = 5;
            $frame = $this->readFrame($listener, $client);

            self::assertSame(Frame::TYPE_HEARTBEAT, $frame->type);
            self::assertSame(0, $frame->channel);
            fclose($client);
        } finally {
            $listener->stop();
        }
    }

    public function testOutboundTrafficDelaysUnnecessaryHeartbeat(): void
    {
        $now = 0;
        $listener = new AmqpListener(
            new ConnectionRegistry(),
            '127.0.0.1',
            0,
            authenticator: $this->authenticator(),
            heartbeatInterval: 5,
            clock: static function () use (&$now): int {
                return $now * 1_000_000_000;
            }
        );
        $listener->start();

        try {
            $client = $this->connect($listener);
            $codec = new FrameCodec();
            $this->connectAmqp($listener, $client, 5);

            $now = 4;
            fwrite($client, $codec->encode(Frame::methodFrame(1, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));

            $now = 8;
            $listener->tick();
            self::assertSame('', fread($client, 8192));

            $now = 9;
            self::assertSame(Frame::TYPE_HEARTBEAT, $this->readFrame($listener, $client)->type);
            fclose($client);
        } finally {
            $listener->stop();
        }
    }

    public function testOneStaleConnectionClosesWhileAnotherActiveConnectionSurvives(): void
    {
        $now = 0;
        $connections = new ConnectionRegistry();
        $listener = new AmqpListener(
            $connections,
            '127.0.0.1',
            0,
            authenticator: $this->authenticator(),
            heartbeatInterval: 5,
            clock: static function () use (&$now): int {
                return $now * 1_000_000_000;
            }
        );
        $listener->start();

        try {
            $staleClient = $this->connect($listener);
            $activeClient = $this->connect($listener);
            $this->connectAmqp($listener, $staleClient, 5);
            $this->connectAmqp($listener, $activeClient, 5);
            self::assertSame(2, $connections->count());

            $now = 9;
            fwrite($activeClient, (new FrameCodec())->encode(Frame::heartbeatFrame()));
            $listener->tick();
            self::assertSame(2, $connections->count());

            $now = 10;
            $listener->tick();

            self::assertSame(1, $connections->count());
            self::assertFalse(feof($activeClient));
            fclose($staleClient);
            fclose($activeClient);
        } finally {
            $listener->stop();
        }
    }

    public function testListenerStopClosesPort(): void
    {
        $listener = new AmqpListener(new ConnectionRegistry(), '127.0.0.1', 0);
        $listener->start();
        $port = $listener->port();

        $listener->stop();

        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $port), $errorCode, $errorMessage, 0.1);
        if (is_resource($client)) {
            fclose($client);
        }

        self::assertFalse($client);
    }

    /**
     * @return resource
     */
    private function connect(AmqpListener $listener): mixed
    {
        $client = @stream_socket_client(
            sprintf('tcp://127.0.0.1:%d', $listener->port()),
            $errorCode,
            $errorMessage,
            1.0
        );
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);

        return $client;
    }

    private function connectTls(AmqpListener $listener): mixed
    {
        $client = $this->connect($listener);
        stream_context_set_options($client, [
            'ssl' => [
                'verify_peer' => false,
                'verify_peer_name' => false,
                'allow_self_signed' => true,
                'crypto_method' => $this->clientCryptoMethod(),
            ],
        ]);

        for ($attempt = 0; $attempt < 200; $attempt++) {
            $listener->tick();
            $result = @stream_socket_enable_crypto($client, true, $this->clientCryptoMethod());
            if ($result === true) {
                return $client;
            }

            if ($result === false) {
                self::fail('TLS handshake failed.');
            }

            usleep(1000);
        }

        self::fail('Timed out waiting for TLS handshake.');
    }

    /**
     * @param resource $client
     * @return array{0: int, 1: int}
     */
    private function readMethod(AmqpListener $listener, mixed $client): array
    {
        return $this->readFrame($listener, $client)->method();
    }

    /**
     * @param resource $client
     */
    private function readFrame(AmqpListener $listener, mixed $client): Frame
    {
        $codec = new FrameCodec();

        for ($attempt = 0; $attempt < 100; $attempt++) {
            $listener->tick();
            $bytes = fread($client, 8192);
            if (is_string($bytes) && $bytes !== '') {
                $frames = $codec->push($bytes);
                if ($frames !== []) {
                    return $frames[0];
                }
            }
            usleep(1000);
        }

        self::fail('Timed out waiting for AMQP frame.');
    }

    /**
     * @param resource $client
     */
    private function connectAmqp(AmqpListener $listener, mixed $client, int $heartbeat): void
    {
        $codec = new FrameCodec();
        fwrite($client, AmqpConnection::PROTOCOL_HEADER);
        self::assertSame([10, 10], $this->readMethod($listener, $client));
        fwrite($client, $codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk())));
        self::assertSame([10, 30], $this->readMethod($listener, $client));
        fwrite($client, $codec->encode(Frame::methodFrame(0, 10, 31, $this->tuneOk($heartbeat))));
        $listener->tick();
        fwrite($client, $codec->encode(Frame::methodFrame(0, 10, 40, $this->connectionOpen('/'))));
        self::assertSame([10, 41], $this->readMethod($listener, $client));
    }

    /**
     * @param resource $client
     */
    private function awaitEof(AmqpListener $listener, mixed $client): void
    {
        for ($attempt = 0; $attempt < 100; $attempt++) {
            $listener->tick();
            fread($client, 8192);
            if (feof($client)) {
                return;
            }
            usleep(1000);
        }

        self::fail('Expected AMQP client socket to close.');
    }

    private function replyCode(Frame $frame): int
    {
        $value = unpack('nreplyCode', substr($frame->payload, 4, 2));

        return (int) $value['replyCode'];
    }

    private function tuneOk(int $heartbeat): string
    {
        return pack('nNn', 0, 131072, $heartbeat);
    }

    private function shortString(string $value): string
    {
        return chr(strlen($value)) . $value;
    }

    private function longString(string $value): string
    {
        return pack('N', strlen($value)) . $value;
    }

    private function startOk(): string
    {
        return pack('N', 0)
            . $this->shortString('PLAIN')
            . $this->longString("\0guest\0guest")
            . $this->shortString('en_US');
    }

    private function connectionOpen(string $virtualHost): string
    {
        return $this->shortString($virtualHost) . $this->shortString('') . "\x00";
    }

    private function authenticator(): AuthenticationService
    {
        return new ListenerAuthenticationService();
    }

    private function tlsConfig(): AmqpTlsConfig
    {
        try {
            $tls = TlsCertificate::create();
        } catch (RuntimeException $exception) {
            self::markTestSkipped($exception->getMessage());
        }

        return new AmqpTlsConfig($tls['cert'], $tls['key']);
    }

    private function clientCryptoMethod(): int
    {
        $method = 0;

        if (defined('STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT')) {
            $method |= STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT;
        }

        if (defined('STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT')) {
            $method |= STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT;
        }

        return $method !== 0 ? $method : STREAM_CRYPTO_METHOD_TLS_CLIENT;
    }
}

final readonly class ListenerAuthenticationService implements AuthenticationService
{
    public function authenticate(string $username, string $password): AuthenticationResult
    {
        if ($username !== 'guest' || $password !== 'guest') {
            return AuthenticationResult::failure();
        }

        return AuthenticationResult::success(new AuthenticatedUser(1, 'guest'));
    }

    public function canAccessVirtualHost(AuthenticatedUser $user, string $virtualHost): bool
    {
        return $virtualHost === '/';
    }
}
