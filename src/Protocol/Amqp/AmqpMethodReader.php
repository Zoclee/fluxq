<?php

declare(strict_types=1);

namespace FluxQ\Protocol\Amqp;

final class AmqpMethodReader
{
    private int $offset = 0;

    public function __construct(
        private readonly string $payload
    ) {
    }

    public function readOctet(): int
    {
        $this->requireBytes(1);

        return ord($this->payload[$this->offset++]);
    }

    public function readShort(): int
    {
        $this->requireBytes(2);
        $value = unpack('nvalue', substr($this->payload, $this->offset, 2));
        $this->offset += 2;

        return (int) $value['value'];
    }

    public function readLong(): int
    {
        $this->requireBytes(4);
        $value = unpack('Nvalue', substr($this->payload, $this->offset, 4));
        $this->offset += 4;

        return (int) $value['value'];
    }

    public function readLongLong(): int
    {
        $this->requireBytes(8);
        $value = unpack('Nhigh/Nlow', substr($this->payload, $this->offset, 8));
        $this->offset += 8;
        $high = (int) $value['high'];
        $low = (int) $value['low'];

        if ($high > intdiv(PHP_INT_MAX, 4294967296)) {
            throw new ProtocolException('AMQP long-long value is too large for this platform.');
        }

        return ($high * 4294967296) + $low;
    }

    public function readShortString(): string
    {
        $length = $this->readOctet();
        $this->requireBytes($length);
        $value = substr($this->payload, $this->offset, $length);
        $this->offset += $length;

        return $value;
    }

    public function readLongString(): string
    {
        $length = $this->readLong();
        $this->requireBytes($length);
        $value = substr($this->payload, $this->offset, $length);
        $this->offset += $length;

        return $value;
    }

    public function skipTable(): void
    {
        $this->requireBytes(4);
        $value = unpack('Nlength', substr($this->payload, $this->offset, 4));
        $this->offset += 4;
        $length = (int) $value['length'];
        $this->requireBytes($length);
        $this->offset += $length;
    }

    /**
     * @return array<string, mixed>
     */
    public function readTable(): array
    {
        $length = $this->readLong();
        $end = $this->offset + $length;
        $this->requireBytes($length);
        $table = [];

        while ($this->offset < $end) {
            $key = $this->readShortString();
            if ($this->offset >= $end) {
                throw new ProtocolException('AMQP field table is malformed.');
            }

            $type = $this->payload[$this->offset++];
            $table[$key] = match ($type) {
                'S' => $this->readLongString(),
                't' => $this->readOctet() !== 0,
                'b' => $this->readSignedOctet(),
                's' => $this->readSignedShort(),
                'I' => $this->readSignedLong(),
                'F' => $this->readTable(),
                'V' => null,
                default => throw new ProtocolException(sprintf('Unsupported AMQP field table type "%s".', $type)),
            };
        }

        if ($this->offset !== $end) {
            throw new ProtocolException('AMQP field table length is invalid.');
        }

        return $table;
    }

    public function assertComplete(): void
    {
        if ($this->offset !== strlen($this->payload)) {
            throw new ProtocolException('AMQP method frame contains trailing bytes.');
        }
    }

    private function requireBytes(int $length): void
    {
        if ($length < 0 || strlen($this->payload) - $this->offset < $length) {
            throw new ProtocolException('AMQP method frame is malformed.');
        }
    }

    private function readSignedOctet(): int
    {
        $value = $this->readOctet();

        return $value > 127 ? $value - 256 : $value;
    }

    private function readSignedShort(): int
    {
        $value = $this->readShort();

        return $value > 32767 ? $value - 65536 : $value;
    }

    private function readSignedLong(): int
    {
        $value = $this->readLong();

        return $value > 2147483647 ? $value - 4294967296 : $value;
    }
}
