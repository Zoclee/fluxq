<?php

declare(strict_types=1);

return [
    'database' => [
        'host' => getenv('FLUXQ_DB_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('FLUXQ_DB_PORT') ?: 5432),
        'name' => getenv('FLUXQ_DB_NAME') ?: 'fluxq',
        'user' => getenv('FLUXQ_DB_USER') ?: 'fluxq',
        'password' => getenv('FLUXQ_DB_PASSWORD') ?: null,
    ],
    'limits' => [
        'max_connections' => (int) (getenv('FLUXQ_MAX_CONNECTIONS') === false ? 1000 : getenv('FLUXQ_MAX_CONNECTIONS')),
        'max_channels_per_connection' => (int) (getenv('FLUXQ_MAX_CHANNELS_PER_CONNECTION') === false ? 256 : getenv('FLUXQ_MAX_CHANNELS_PER_CONNECTION')),
        'max_consumers_per_connection' => (int) (getenv('FLUXQ_MAX_CONSUMERS_PER_CONNECTION') === false ? 256 : getenv('FLUXQ_MAX_CONSUMERS_PER_CONNECTION')),
        'max_consumers_per_channel' => (int) (getenv('FLUXQ_MAX_CONSUMERS_PER_CHANNEL') === false ? 64 : getenv('FLUXQ_MAX_CONSUMERS_PER_CHANNEL')),
        'amqp_max_frame_size' => (int) (getenv('FLUXQ_AMQP_MAX_FRAME_SIZE') === false ? 1048576 : getenv('FLUXQ_AMQP_MAX_FRAME_SIZE')),
        'max_message_size' => (int) (getenv('FLUXQ_MAX_MESSAGE_SIZE') === false ? 16777216 : getenv('FLUXQ_MAX_MESSAGE_SIZE')),
        'max_queues_per_virtual_host' => (int) (getenv('FLUXQ_MAX_QUEUES_PER_VHOST') === false ? 10000 : getenv('FLUXQ_MAX_QUEUES_PER_VHOST')),
        'max_queue_depth' => (int) (getenv('FLUXQ_MAX_QUEUE_DEPTH') === false ? 1000000 : getenv('FLUXQ_MAX_QUEUE_DEPTH')),
    ],
    'amqp' => [
        'enabled' => filter_var(getenv('FLUXQ_AMQP_ENABLED') === false ? true : getenv('FLUXQ_AMQP_ENABLED'), FILTER_VALIDATE_BOOL),
        'host' => getenv('FLUXQ_AMQP_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('FLUXQ_AMQP_PORT') ?: 5672),
        'heartbeat' => (int) (getenv('FLUXQ_AMQP_HEARTBEAT') === false ? 60 : getenv('FLUXQ_AMQP_HEARTBEAT')),
        'tls' => [
            'enabled' => filter_var(getenv('FLUXQ_AMQP_TLS_ENABLED') === false ? false : getenv('FLUXQ_AMQP_TLS_ENABLED'), FILTER_VALIDATE_BOOL),
            'host' => getenv('FLUXQ_AMQP_TLS_HOST') ?: '0.0.0.0',
            'port' => (int) (getenv('FLUXQ_AMQP_TLS_PORT') ?: 5671),
            'cert' => getenv('FLUXQ_AMQP_TLS_CERT') ?: '',
            'key' => getenv('FLUXQ_AMQP_TLS_KEY') ?: '',
            'ca' => getenv('FLUXQ_AMQP_TLS_CA') ?: null,
        ],
    ],
    'diagnostics' => [
        'host' => getenv('FLUXQ_DIAGNOSTICS_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('FLUXQ_DIAGNOSTICS_PORT') ?: 5673),
    ],
    'shutdown' => [
        'drain_timeout' => (int) (getenv('FLUXQ_SHUTDOWN_DRAIN_TIMEOUT') === false ? 30 : getenv('FLUXQ_SHUTDOWN_DRAIN_TIMEOUT')),
    ],
];
