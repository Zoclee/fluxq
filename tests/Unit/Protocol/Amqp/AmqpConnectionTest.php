<?php

declare(strict_types=1);

namespace FluxQ\Tests\Unit\Protocol\Amqp;

use FluxQ\Broker\AuthenticatedUser;
use FluxQ\Broker\AuthenticationResult;
use FluxQ\Broker\AuthenticationService;
use FluxQ\Protocol\Amqp\AmqpConnection;
use FluxQ\Protocol\Amqp\AmqpConnectionState;
use FluxQ\Protocol\Amqp\Frame;
use FluxQ\Protocol\Amqp\FrameCodec;
use FluxQ\Protocol\Amqp\AmqpMethodReader;
use FluxQ\Protocol\Amqp\ProtocolException;
use FluxQ\Runtime\ConnectionRegistry;
use PHPUnit\Framework\TestCase;

final class AmqpConnectionTest extends TestCase
{
    public function testProtocolHeaderStartsHandshake(): void
    {
        [$connection, $socket] = $this->connection();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);

        self::assertSame(AmqpConnectionState::Starting, $connection->state());
        self::assertSame([10, 10], $this->writtenMethods($socket)[0]);
    }

    public function testConnectionStartAdvertisesPublisherConfirms(): void
    {
        [$connection, $socket] = $this->connection();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);

        $properties = $this->serverPropertiesFromConnectionStart($socket);

        self::assertSame(
            [
                'basic.nack' => true,
                'publisher_confirms' => true,
            ],
            $properties['capabilities'] ?? null
        );
    }

    public function testProtocolHeaderCanArrivePartially(): void
    {
        [$connection, $socket] = $this->connection();

        $connection->receive(substr(AmqpConnection::PROTOCOL_HEADER, 0, 4));
        self::assertSame(AmqpConnectionState::AwaitingProtocolHeader, $connection->state());

        $connection->receive(substr(AmqpConnection::PROTOCOL_HEADER, 4));

        self::assertSame(AmqpConnectionState::Starting, $connection->state());
        self::assertSame([10, 10], $this->writtenMethods($socket)[0]);
    }

    public function testInvalidProtocolHeaderIsRejected(): void
    {
        [$connection] = $this->connection();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Unsupported AMQP protocol header.');

        $connection->receive("AMQP\x00\x00\x09\x00");
    }

    public function testConnectionHandshakeStateTransitions(): void
    {
        [$connection, $socket] = $this->connection();
        $codec = new FrameCodec();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk())));
        self::assertSame(AmqpConnectionState::Tuning, $connection->state());

        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 31, $this->tuneOk(60))));
        self::assertSame(AmqpConnectionState::Opening, $connection->state());
        self::assertSame(60, $connection->negotiatedHeartbeatInterval());

        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 40, $this->connectionOpen('/'))));
        self::assertSame(AmqpConnectionState::Open, $connection->state());

        self::assertSame([[10, 10], [10, 30], [10, 41]], $this->writtenMethods($socket));
    }

    public function testUnexpectedMethodForStateIsRejected(): void
    {
        [$connection] = $this->connection();
        $codec = new FrameCodec();
        $connection->receive(AmqpConnection::PROTOCOL_HEADER);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Unexpected AMQP method for current connection state.');

        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 40)));
    }

    public function testHandshakeMethodsMustUseChannelZero(): void
    {
        [$connection] = $this->connection();
        $codec = new FrameCodec();
        $connection->receive(AmqpConnection::PROTOCOL_HEADER);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('AMQP connection handshake must use channel 0.');

        $connection->receive($codec->encode(Frame::methodFrame(1, 10, 11)));
    }

    public function testChannelCanOpenAndCloseAfterConnectionHandshake(): void
    {
        [$connection, $socket] = $this->connection();
        $codec = new FrameCodec();
        $this->completeHandshake($connection);

        $connection->receive($codec->encode(Frame::methodFrame(1, 20, 10, "\x00")));
        $connection->receive($codec->encode(Frame::methodFrame(1, 20, 40, pack('n', 200) . "\x00" . pack('nn', 0, 0))));

        self::assertSame(
            [[10, 10], [10, 30], [10, 41], [20, 11], [20, 41]],
            $this->writtenMethods($socket)
        );
    }

    public function testMultipleChannelsCanBeOpenedOnOneConnection(): void
    {
        [$connection, $socket] = $this->connection();
        $codec = new FrameCodec();
        $this->completeHandshake($connection);

        $connection->receive($codec->encode(Frame::methodFrame(1, 20, 10, "\x00")));
        $connection->receive($codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));

        self::assertSame(
            [[10, 10], [10, 30], [10, 41], [20, 11], [20, 11]],
            $this->writtenMethods($socket)
        );
    }

    public function testOperationOnUnopenedChannelGetsChannelClose(): void
    {
        [$connection, $socket] = $this->connection();
        $codec = new FrameCodec();
        $this->completeHandshake($connection);

        $connection->receive($codec->encode(Frame::methodFrame(9, 50, 10, pack('n', 0) . "\x06orders" . "\x00" . pack('N', 0))));

        $methods = $this->writtenMethods($socket);
        self::assertSame([20, 40], $methods[count($methods) - 1]);
    }

    public function testMalformedMethodFrameIsRejected(): void
    {
        [$connection] = $this->connection();
        $this->completeHandshake($connection);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Expected a complete AMQP method frame.');

        $connection->receive((new FrameCodec())->encode(new Frame(Frame::TYPE_METHOD, 1, "\x00")));
    }

    public function testHeartbeatValueNegotiatesToLowerNonZeroInterval(): void
    {
        [$connection, $socket] = $this->connection(60);
        $codec = new FrameCodec();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk())));
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 31, $this->tuneOk(10))));

        self::assertSame(10, $connection->negotiatedHeartbeatInterval());
        self::assertSame(60, $this->heartbeatFromTune($socket));
    }

    public function testHeartbeatCanBeDisabledWithZero(): void
    {
        [$connection] = $this->connection(60);
        $codec = new FrameCodec();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk())));
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 31, $this->tuneOk(0))));

        self::assertSame(0, $connection->negotiatedHeartbeatInterval());
    }

    public function testMalformedPlainResponseClosesConnectionBeforeTune(): void
    {
        [$connection, $peer] = $this->connectionPair();
        $codec = new FrameCodec();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk(response: 'guest'))));

        self::assertSame(AmqpConnectionState::Closed, $connection->state());
        self::assertSame([[10, 10], [10, 50]], $this->readableWrittenMethods($peer));
    }

    public function testUnsupportedSaslMechanismClosesConnectionBeforeTune(): void
    {
        [$connection, $peer] = $this->connectionPair();
        $codec = new FrameCodec();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk(mechanism: 'AMQPLAIN'))));

        self::assertSame(AmqpConnectionState::Closed, $connection->state());
        self::assertSame([[10, 10], [10, 50]], $this->readableWrittenMethods($peer));
    }

    public function testInvalidCredentialsCloseConnectionBeforeTune(): void
    {
        [$connection, $peer] = $this->connectionPair();
        $codec = new FrameCodec();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(
            0,
            10,
            11,
            $this->startOk(response: "\0guest\0wrong")
        )));

        self::assertSame(AmqpConnectionState::Closed, $connection->state());
        self::assertSame([[10, 10], [10, 50]], $this->readableWrittenMethods($peer));
    }

    public function testUnauthorizedVirtualHostNeverReceivesOpenOk(): void
    {
        [$connection, $peer] = $this->connectionPair();
        $codec = new FrameCodec();

        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk())));
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 31, $this->tuneOk(60))));
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 40, $this->connectionOpen('tenant-a'))));

        self::assertSame(AmqpConnectionState::Closed, $connection->state());
        self::assertSame([[10, 10], [10, 30], [10, 50]], $this->readableWrittenMethods($peer));
    }

    public function testMalformedHeartbeatFrameIsRejected(): void
    {
        [$connection] = $this->connection();
        $this->completeHandshake($connection);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('AMQP heartbeat frames must use channel 0 with an empty payload.');

        $connection->receive((new FrameCodec())->encode(new Frame(Frame::TYPE_HEARTBEAT, 1, 'x')));
    }

    /**
     * @return array{0: AmqpConnection, 1: resource}
     */
    private function connection(int $heartbeatInterval = 60): array
    {
        $socket = fopen('php://temp', 'w+');
        self::assertIsResource($socket);

        return [
            new AmqpConnection(
                $socket,
                new ConnectionRegistry(),
                authenticator: new InMemoryAuthenticationService(),
                heartbeatInterval: $heartbeatInterval
            ),
            $socket,
        ];
    }

    /**
     * @return array{0: AmqpConnection, 1: resource}
     */
    private function connectionPair(): array
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        self::assertIsResource($server, sprintf('Could not create test socket server: %s (%d)', $errorMessage, $errorCode));

        $address = stream_socket_get_name($server, false);
        self::assertIsString($address);

        $peer = stream_socket_client('tcp://' . $address);
        self::assertIsResource($peer);

        $socket = stream_socket_accept($server, 1);
        fclose($server);
        self::assertIsResource($socket);
        stream_set_blocking($peer, false);

        return [
            new AmqpConnection(
                $socket,
                new ConnectionRegistry(),
                authenticator: new InMemoryAuthenticationService()
            ),
            $peer,
        ];
    }

    /**
     * @param resource $socket
     * @return list<array{0: int, 1: int}>
     */
    private function writtenMethods(mixed $socket): array
    {
        rewind($socket);
        $bytes = stream_get_contents($socket);
        self::assertIsString($bytes);

        $methods = [];
        $codec = new FrameCodec();
        foreach ($codec->push($bytes) as $frame) {
            $methods[] = $frame->method();
        }

        return $methods;
    }

    /**
     * @param resource $socket
     * @return list<array{0: int, 1: int}>
     */
    private function readableWrittenMethods(mixed $socket): array
    {
        $bytes = '';
        while (!feof($socket)) {
            $chunk = fread($socket, 8192);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $bytes .= $chunk;
        }

        $methods = [];
        $codec = new FrameCodec();
        foreach ($codec->push($bytes) as $frame) {
            $methods[] = $frame->method();
        }

        return $methods;
    }

    /**
     * @param resource $socket
     * @return array<string, mixed>
     */
    private function serverPropertiesFromConnectionStart(mixed $socket): array
    {
        rewind($socket);
        $bytes = stream_get_contents($socket);
        self::assertIsString($bytes);

        foreach ((new FrameCodec())->push($bytes) as $frame) {
            if ($frame->method() !== [10, 10]) {
                continue;
            }

            $reader = new AmqpMethodReader(substr($frame->payload, 4));
            $reader->readOctet();
            $reader->readOctet();

            return $reader->readTable();
        }

        self::fail('connection.start was not written.');
    }

    private function completeHandshake(AmqpConnection $connection): void
    {
        $codec = new FrameCodec();
        $connection->receive(AmqpConnection::PROTOCOL_HEADER);
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk())));
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 31, $this->tuneOk(60))));
        $connection->receive($codec->encode(Frame::methodFrame(0, 10, 40, $this->connectionOpen('/'))));
        self::assertSame(AmqpConnectionState::Open, $connection->state());
    }

    private function startOk(string $mechanism = 'PLAIN', ?string $response = null): string
    {
        $response ??= "\0guest\0guest";

        return pack('N', 0)
            . $this->shortString($mechanism)
            . $this->longString($response)
            . $this->shortString('en_US');
    }

    private function connectionOpen(string $virtualHost): string
    {
        return $this->shortString($virtualHost) . $this->shortString('') . "\x00";
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

    /**
     * @param resource $socket
     */
    private function heartbeatFromTune(mixed $socket): int
    {
        rewind($socket);
        $bytes = stream_get_contents($socket);
        self::assertIsString($bytes);

        foreach ((new FrameCodec())->push($bytes) as $frame) {
            if ($frame->method() !== [10, 30]) {
                continue;
            }

            $values = unpack('nchannelMax/NframeMax/nheartbeat', substr($frame->payload, 4, 8));

            return (int) $values['heartbeat'];
        }

        self::fail('connection.tune was not written.');
    }
}

final readonly class InMemoryAuthenticationService implements AuthenticationService
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
