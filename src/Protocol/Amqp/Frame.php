<?php

declare(strict_types=1);

namespace FluxQ\Protocol\Amqp;

use InvalidArgumentException;

final readonly class Frame
{
    public const TYPE_METHOD = 1;
    public const TYPE_HEADER = 2;
    public const TYPE_BODY = 3;
    public const TYPE_HEARTBEAT = 8;

    public function __construct(
        public int $type,
        public int $channel,
        public string $payload
    ) {
        if ($this->type < 1 || $this->type > 255) {
            throw new InvalidArgumentException('AMQP frame type must fit in one octet.');
        }

        if ($this->channel < 0 || $this->channel > 65535) {
            throw new InvalidArgumentException('AMQP frame channel must fit in an unsigned short.');
        }
    }

    public static function methodFrame(int $channel, int $classId, int $methodId, string $arguments = ''): self
    {
        return new self(self::TYPE_METHOD, $channel, pack('nn', $classId, $methodId) . $arguments);
    }

    public static function heartbeatFrame(): self
    {
        return new self(self::TYPE_HEARTBEAT, 0, '');
    }

    /**
     * @return array{0: int, 1: int}
     */
    public function method(): array
    {
        if ($this->type !== self::TYPE_METHOD || strlen($this->payload) < 4) {
            throw new ProtocolException('Expected a complete AMQP method frame.');
        }

        $method = unpack('nclass/nmethod', substr($this->payload, 0, 4));

        return [(int) $method['class'], (int) $method['method']];
    }
}
