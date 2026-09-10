<?php

declare(strict_types=1);

namespace FluxQ\Protocol\Amqp;

final class FrameCodec
{
    public const FRAME_END = "\xCE";

    private string $buffer = '';

    public function __construct(
        private readonly int $maxFrameSize = 131072
    ) {
    }

    public function encode(Frame $frame): string
    {
        $size = strlen($frame->payload);
        if ($this->maxFrameSize !== 0 && $size > $this->maxFrameSize) {
            throw new ProtocolException('AMQP frame exceeds configured frame size limit.');
        }

        return pack('CnN', $frame->type, $frame->channel, $size) . $frame->payload . self::FRAME_END;
    }

    /**
     * @return list<Frame>
     */
    public function push(string $bytes): array
    {
        if ($bytes === '') {
            return [];
        }

        $this->buffer .= $bytes;
        $frames = [];

        while (strlen($this->buffer) >= 7) {
            $header = unpack('Ctype/nchannel/Nsize', substr($this->buffer, 0, 7));
            $size = (int) $header['size'];

            if ($this->maxFrameSize !== 0 && $size > $this->maxFrameSize) {
                throw new ProtocolException('AMQP frame exceeds configured frame size limit.');
            }

            $frameSize = 7 + $size + 1;
            if (strlen($this->buffer) < $frameSize) {
                break;
            }

            if ($this->buffer[$frameSize - 1] !== self::FRAME_END) {
                throw new ProtocolException('AMQP frame end marker is invalid.');
            }

            $frames[] = new Frame(
                (int) $header['type'],
                (int) $header['channel'],
                substr($this->buffer, 7, $size)
            );
            $this->buffer = substr($this->buffer, $frameSize);
        }

        return $frames;
    }

    public function bufferedByteCount(): int
    {
        return strlen($this->buffer);
    }
}
