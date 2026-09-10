<?php

declare(strict_types=1);

namespace FluxQ\Broker;

use RuntimeException;

final readonly class ResourceLimits
{
    public function __construct(
        public int $maxConnections = 1000,
        public int $maxChannelsPerConnection = 256,
        public int $maxConsumersPerConnection = 256,
        public int $maxConsumersPerChannel = 64,
        public int $maxFrameSize = 1048576,
        public int $maxMessageSize = 16777216,
        public int $maxQueuesPerVirtualHost = 10000,
        public int $maxQueueDepth = 1000000
    ) {
        foreach (get_object_vars($this) as $name => $value) {
            if ($value < 0) {
                throw new RuntimeException(sprintf('Resource limit "%s" must be zero or greater.', $name));
            }
        }
    }

    /**
     * @param array<string, mixed> $config
     */
    public static function fromArray(array $config): self
    {
        return new self(
            maxConnections: (int) ($config['max_connections'] ?? 1000),
            maxChannelsPerConnection: (int) ($config['max_channels_per_connection'] ?? 256),
            maxConsumersPerConnection: (int) ($config['max_consumers_per_connection'] ?? 256),
            maxConsumersPerChannel: (int) ($config['max_consumers_per_channel'] ?? 64),
            maxFrameSize: (int) ($config['amqp_max_frame_size'] ?? 1048576),
            maxMessageSize: (int) ($config['max_message_size'] ?? 16777216),
            maxQueuesPerVirtualHost: (int) ($config['max_queues_per_virtual_host'] ?? 10000),
            maxQueueDepth: (int) ($config['max_queue_depth'] ?? 1000000),
        );
    }

    public static function unlimited(): self
    {
        return new self(0, 0, 0, 0, 0, 0, 0, 0);
    }

    public function allows(int $limit, int $current, int $additional = 1): bool
    {
        return $limit === 0 || $current + $additional <= $limit;
    }
}
