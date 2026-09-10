<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Protocol\Amqp;

use FluxQ\Broker\Broker;
use FluxQ\Broker\Authorizer;
use FluxQ\Broker\Authenticator;
use FluxQ\Broker\DestinationType;
use FluxQ\Broker\PublishRequest;
use FluxQ\Broker\ResourceLimitException;
use FluxQ\Broker\ResourceLimits;
use FluxQ\Persistence\Postgres\BindingRepository;
use FluxQ\Persistence\Postgres\Connection;
use FluxQ\Persistence\Postgres\DeliveryRepository;
use FluxQ\Persistence\Postgres\DestinationRepository;
use FluxQ\Persistence\Postgres\MessageRepository;
use FluxQ\Persistence\Postgres\MessageRouteRepository;
use FluxQ\Persistence\Postgres\Migrator;
use FluxQ\Persistence\Postgres\PublishTransaction;
use FluxQ\Persistence\Postgres\RoutingSourceRepository;
use FluxQ\Persistence\Postgres\SubscriptionRepository;
use FluxQ\Persistence\Postgres\UserRepository;
use FluxQ\Persistence\Postgres\VirtualHostRepository;
use FluxQ\Protocol\Amqp\AmqpConnection;
use FluxQ\Protocol\Amqp\AmqpListener;
use FluxQ\Protocol\Amqp\Frame;
use FluxQ\Protocol\Amqp\FrameCodec;
use FluxQ\Runtime\ConnectionRegistry;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;

final class AmqpTopologyTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private int $virtualHostId;

    #[Before]
    public function setUpDatabase(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for AMQP topology integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');
        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run AMQP topology integration tests.');
        }

        $this->connection = Connection::fromDsn($dsn);
        $this->pdo = $this->connection->pdo();
        $this->assertSafeTestDatabase();
        $this->resetSchema();
        (new Migrator($this->connection, dirname(__DIR__, 4) . '/database/migrations'))->migrate();
        $this->virtualHostId = (new VirtualHostRepository($this->connection))->findByName('/')?->id
            ?? throw new \RuntimeException('Default virtual host was not created by migrations.');
        $users = new UserRepository($this->connection);
        $users->create('guest', 'guest');
        $users->grantVirtualHost('guest', '/');
        $users->setPermissions('guest', '/', '.*', '.*', '.*');
    }

    public function testAmqpCanDeclareQueueExchangeBindAndCloseChannel(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);

            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('orders', durable: true))));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('orders', durable: true))));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('orders.direct', 'direct', durable: true))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 20, $this->queueBind('orders', 'orders.direct', 'created'))));
            self::assertSame([50, 21], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 20, $this->queueBind('orders', '', 'orders'))));
            self::assertSame([50, 21], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 20, 40, pack('n', 200) . "\x00" . pack('nn', 0, 0))));
            self::assertSame([20, 41], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        $queue = (new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'orders');
        self::assertNotNull($queue);
        self::assertSame(DestinationType::Queue, $queue->type);
        self::assertTrue($queue->durable);

        $source = (new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'orders.direct');
        self::assertNotNull($source);
        self::assertSame('direct', $source->type->value);

        $bindings = (new BindingRepository($this->connection))->findForRoute($this->virtualHostId, 'orders.direct', 'created');
        self::assertCount(1, $bindings);
        self::assertSame($queue->id, $bindings[0]->destinationId);
    }

    public function testIncompatibleQueueRedeclarationClosesChannelButConnectionSurvives(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('orders', durable: true))));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('orders', durable: false))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));

            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPassiveQueueDeclareIgnoresIncomingPropertiesAndDoesNotMutateQueue(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                50,
                10,
                $this->queueDeclare('test_durable_queue', durable: true, autoDelete: false)
            )));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                50,
                10,
                $this->queueDeclare(
                    'test_durable_queue',
                    durable: false,
                    passive: true,
                    exclusive: true,
                    autoDelete: true
                )
            )));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('after.passive', 'direct'))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        $queue = (new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'test_durable_queue');

        self::assertNotNull($queue);
        self::assertTrue($queue->durable);
        self::assertFalse($queue->autoDelete);
    }

    public function testPassiveQueueDeclareForMissingQueueClosesChannelWithNotFoundAndDoesNotCreateQueue(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                50,
                10,
                $this->queueDeclare('missing', durable: false, passive: true)
            )));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(404, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'missing'));
    }

    public function testUnsupportedExchangeTypeClosesChannel(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('orders.headers', 'headers'))));

            self::assertSame([20, 40], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testFanoutExchangeDeclareAndCompatibleRedeclare(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'fanout', durable: true))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'fanout', durable: true))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        $source = (new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'events');
        self::assertNotNull($source);
        self::assertSame('fanout', $source->type->value);
        self::assertTrue($source->durable);
    }

    public function testTopicExchangeDeclareAndCompatibleRedeclare(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events.topic', 'topic', durable: true))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events.topic', 'topic', durable: true))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        $source = (new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'events.topic');
        self::assertNotNull($source);
        self::assertSame('topic', $source->type->value);
        self::assertTrue($source->durable);
    }

    public function testPassiveExchangeDeclareIgnoresDurabilityAutoDeleteAndInternalFlagsWithoutMutatingSource(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                40,
                10,
                $this->exchangeDeclare('test_durable_exchange', 'direct', durable: true)
            )));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                40,
                10,
                $this->exchangeDeclare(
                    'test_durable_exchange',
                    'direct',
                    durable: false,
                    passive: true,
                    autoDelete: true,
                    internal: true
                )
            )));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('after-passive', durable: true))));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        $source = (new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'test_durable_exchange');

        self::assertNotNull($source);
        self::assertSame('direct', $source->type->value);
        self::assertTrue($source->durable);
        self::assertFalse($source->autoDelete);
        self::assertSame(['declared_by' => 'topology'], $source->metadata);
    }

    public function testPassiveExchangeDeclareWithMatchingDirectFanoutAndTopicTypesSucceeds(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);

            foreach ([
                'events.direct' => 'direct',
                'events.fanout' => 'fanout',
                'events.topic' => 'topic',
            ] as $exchange => $type) {
                fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare($exchange, $type, durable: true))));
                self::assertSame([40, 11], $this->readMethod($listener, $client));

                fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare($exchange, $type, passive: true))));
                self::assertSame([40, 11], $this->readMethod($listener, $client));
            }
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPassiveExchangeDeclareWithWrongTypeClosesChannelWithPreconditionFailed(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'direct', durable: true))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'fanout', passive: true))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPassiveExchangeDeclareForMissingExchangeClosesChannelWithNotFoundAndDoesNotCreateSource(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('missing', 'direct', passive: true))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(404, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertNull((new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'missing'));
    }

    public function testActiveExchangeRedeclarationWithIncompatibleDurabilityStillFails(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'direct', durable: true))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'direct', durable: false))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPassiveDurableExchangeDeclareSurvivesRestartAndBindingStillRoutes(): void
    {
        $codec = new FrameCodec();
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                40,
                10,
                $this->exchangeDeclare('test_durable_exchange', 'direct', durable: true)
            )));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('test_durable_queue', durable: true))));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                50,
                20,
                $this->queueBind('test_durable_queue', 'test_durable_exchange', 'created')
            )));
            self::assertSame([50, 21], $this->readMethod($listener, $client));

            $this->broker()->publish(new PublishRequest('/', 'test_durable_exchange', 'created', 'before-restart'));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->tableCount('message_routes'));
            self::assertSame(1, $this->tableCount('deliveries'));
        } finally {
            fclose($client);
            $listener->stop();
        }

        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                40,
                10,
                $this->exchangeDeclare('test_durable_exchange', 'direct', passive: true)
            )));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(
                1,
                50,
                10,
                $this->queueDeclare('test_durable_queue', durable: false, passive: true)
            )));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $source = (new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'test_durable_exchange');
            $queue = (new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'test_durable_queue');
            self::assertNotNull($source);
            self::assertNotNull($queue);
            self::assertTrue($source->durable);
            self::assertTrue($queue->durable);
            self::assertCount(
                1,
                (new BindingRepository($this->connection))->findForRoute($this->virtualHostId, 'test_durable_exchange', 'created')
            );
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->tableCount('message_routes'));
            self::assertSame(1, $this->tableCount('deliveries'));

            $this->broker()->publish(new PublishRequest('/', 'test_durable_exchange', 'created', 'after-restart'));

            self::assertSame(2, $this->tableCount('messages'));
            self::assertSame(2, $this->tableCount('message_routes'));
            self::assertSame(2, $this->tableCount('deliveries'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testIncompatibleDirectFanoutRedeclarationClosesChannel(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'direct'))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'fanout'))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testIncompatibleTopicRedeclarationClosesChannel(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'topic'))));
            self::assertSame([40, 11], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('events', 'direct'))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedQueueDeclareClosesChannelWithAccessRefused(): void
    {
        (new UserRepository($this->connection))->setPermissions('guest', '/', '^allowed$', '.*', '.*');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('orders', durable: true))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedExchangeDeclareClosesChannelWithAccessRefused(): void
    {
        (new UserRepository($this->connection))->setPermissions('guest', '/', '^allowed$', '.*', '.*');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 10, $this->exchangeDeclare('orders.direct', 'direct'))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedBindingClosesChannelWithAccessRefused(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        $this->broker()->declareDirectRoutingSource('/', 'orders.direct', true, false);
        (new UserRepository($this->connection))->setPermissions('guest', '/', '^orders$', '.*', '.*');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 20, $this->queueBind('orders', 'orders.direct', 'created'))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueCountLimitRejectsNewQueuesButAllowsCompatibleRedeclare(): void
    {
        $broker = $this->broker(new ResourceLimits(maxQueuesPerVirtualHost: 1));

        $first = $broker->declareQueue('/', 'orders', true, false);
        $again = $broker->declareQueue('/', 'orders', true, false);

        self::assertSame($first->id, $again->id);
        self::assertSame(1, (new DestinationRepository($this->connection))->countQueuesByVirtualHost($this->virtualHostId));

        $this->expectException(ResourceLimitException::class);
        $this->expectExceptionMessage('Queue limit reached');

        $broker->declareQueue('/', 'invoices', true, false);
    }

    public function testQueueCountLimitDeniedOverAmqpClosesChannelWithResourceError(): void
    {
        [$listener, $client] = $this->startedListener(new ResourceLimits(maxQueuesPerVirtualHost: 1));
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('orders', true))));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('invoices', true))));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(506, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueuePurgeReturnsCountAndDoesNotAffectAnotherDestinationRoute(): void
    {
        $orders = $this->broker()->declareQueue('/', 'orders', true, false);
        $audit = $this->broker()->declareQueue('/', 'audit', true, false);
        $this->createDelivery($orders->id, 'orders-payload');
        $this->createDelivery($audit->id, 'audit-payload');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 30, $this->queuePurge('orders'))));
            $purgeOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 31], $purgeOk->method());
            self::assertSame(1, $this->messageCount($purgeOk));
            self::assertSame(1, $this->deliveryStateCount($orders->id, 'rejected'));
            self::assertSame(1, $this->deliveryStateCount($audit->id, 'pending'));
            self::assertSame(2, $this->tableCount('messages'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedQueuePurgeClosesChannelWithAccessRefused(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        (new UserRepository($this->connection))->setPermissions('guest', '/', '^$', '.*', '.*');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 30, $this->queuePurge('orders'))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeleteRemovesQueueGraphButKeepsSharedPayload(): void
    {
        $orders = $this->broker()->declareQueue('/', 'orders', true, false);
        $audit = $this->broker()->declareQueue('/', 'audit', true, false);
        $message = (new MessageRepository($this->connection))->create('shared');
        $routes = new MessageRouteRepository($this->connection);
        $ordersRoute = $routes->create($message->id, $orders->id);
        $auditRoute = $routes->create($message->id, $audit->id);
        $subscriptions = new SubscriptionRepository($this->connection);
        $ordersSubscription = $subscriptions->findByName($orders->id, 'amqp') ?? $subscriptions->create($orders->id, 'amqp');
        $auditSubscription = $subscriptions->findByName($audit->id, 'amqp') ?? $subscriptions->create($audit->id, 'amqp');
        $deliveries = new DeliveryRepository($this->connection);
        $deliveries->create($ordersRoute->id, $ordersSubscription->id);
        $deliveries->create($auditRoute->id, $auditSubscription->id);
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 40, $this->queueDelete('orders'))));
            $deleteOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 41], $deleteOk->method());
            self::assertSame(1, $this->messageCount($deleteOk));
            self::assertNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'orders'));
            self::assertNotNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'audit'));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->deliveryStateCount($audit->id, 'pending'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeleteForMissingQueueIsIdempotent(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 40, $this->queueDelete('missing'))));
            $deleteOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 41], $deleteOk->method());
            self::assertSame(1, $deleteOk->channel);
            self::assertSame(0, $this->messageCount($deleteOk));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 10, $this->queueDeclare('after.missing.delete', false))));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeleteIfEmptyRejectsNonEmptyQueue(): void
    {
        $orders = $this->broker()->declareQueue('/', 'orders', true, false);
        $this->createDelivery($orders->id, 'payload');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 40, $this->queueDelete('orders', ifEmpty: true))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
            self::assertNotNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'orders'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeleteNoWaitDeletesWithoutOk(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 40, $this->queueDelete('orders', noWait: true))));
            $listener->tick();

            self::assertNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'orders'));
            $this->assertNoFrame($listener, $client);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueUnbindStopsSubsequentDirectRouting(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        $this->broker()->declareDirectRoutingSource('/', 'orders.direct', true, false);
        $this->broker()->bindQueue('/', 'orders.direct', 'orders', 'created');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 50, $this->queueUnbind('orders', 'orders.direct', 'created'))));
            self::assertSame([50, 51], $this->readMethod($listener, $client));

            $this->broker()->publish(new PublishRequest('/', 'orders.direct', 'created', 'payload'));
            self::assertSame(0, $this->tableCount('message_routes'));
            self::assertSame(0, $this->tableCount('deliveries'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDefaultExchangeCannotBeUnbound(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 50, 50, $this->queueUnbind('orders', '', 'orders'))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testExchangeDeleteRemovesDirectExchangeAndBindings(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        $this->broker()->declareDirectRoutingSource('/', 'orders.direct', true, false);
        $this->broker()->bindQueue('/', 'orders.direct', 'orders', 'created');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 20, $this->exchangeDelete('orders.direct'))));

            self::assertSame([40, 21], $this->readMethod($listener, $client));
            self::assertNull((new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'orders.direct'));
            self::assertSame(0, $this->tableCount('bindings'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testExchangeDeleteIfUnusedRejectsBoundExchange(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        $this->broker()->declareDirectRoutingSource('/', 'orders.direct', true, false);
        $this->broker()->bindQueue('/', 'orders.direct', 'orders', 'created');
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 20, $this->exchangeDelete('orders.direct', ifUnused: true))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
            self::assertNotNull((new RoutingSourceRepository($this->connection))->findByName($this->virtualHostId, 'orders.direct'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDefaultExchangeCannotBeDeleted(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(1, 40, 20, $this->exchangeDelete(''))));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    /**
     * @return array{0: AmqpListener, 1: resource}
     */
    private function startedListener(?ResourceLimits $limits = null): array
    {
        $listener = new AmqpListener(
            new ConnectionRegistry(),
            '127.0.0.1',
            0,
            broker: $this->broker($limits),
            authenticator: $this->authenticator(),
            authorizer: $this->authorizer()
        );
        $listener->start();
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);

        return [$listener, $client];
    }

    /**
     * @param resource $client
     */
    private function connectAndOpenChannel(AmqpListener $listener, mixed $client, int $channel): void
    {
        $codec = new FrameCodec();
        fwrite($client, AmqpConnection::PROTOCOL_HEADER);
        self::assertSame([10, 10], $this->readMethod($listener, $client));
        fwrite($client, $codec->encode(Frame::methodFrame(0, 10, 11, $this->startOk())));
        self::assertSame([10, 30], $this->readMethod($listener, $client));
        fwrite($client, $codec->encode(Frame::methodFrame(0, 10, 31, $this->tuneOk(60))));
        $listener->tick();
        fwrite($client, $codec->encode(Frame::methodFrame(0, 10, 40, $this->connectionOpen('/'))));
        self::assertSame([10, 41], $this->readMethod($listener, $client));
        fwrite($client, $codec->encode(Frame::methodFrame($channel, 20, 10, "\x00")));
        self::assertSame([20, 11], $this->readMethod($listener, $client));
    }

    private function queueDeclare(
        string $queue,
        bool $durable,
        bool $passive = false,
        bool $exclusive = false,
        bool $autoDelete = false
    ): string
    {
        $bits = 0;
        if ($passive) {
            $bits |= 0b00000001;
        }
        if ($durable) {
            $bits |= 0b00000010;
        }
        if ($exclusive) {
            $bits |= 0b00000100;
        }
        if ($autoDelete) {
            $bits |= 0b00001000;
        }

        return pack('n', 0) . $this->shortString($queue) . chr($bits) . pack('N', 0);
    }

    private function exchangeDeclare(
        string $exchange,
        string $type,
        bool $durable = false,
        bool $passive = false,
        bool $autoDelete = false,
        bool $internal = false
    ): string
    {
        $bits = 0;
        if ($passive) {
            $bits |= 0b00000001;
        }
        if ($durable) {
            $bits |= 0b00000010;
        }
        if ($autoDelete) {
            $bits |= 0b00000100;
        }
        if ($internal) {
            $bits |= 0b00001000;
        }

        return pack('n', 0) . $this->shortString($exchange) . $this->shortString($type) . chr($bits) . pack('N', 0);
    }

    private function queueBind(string $queue, string $exchange, string $routingKey): string
    {
        return pack('n', 0)
            . $this->shortString($queue)
            . $this->shortString($exchange)
            . $this->shortString($routingKey)
            . "\x00"
            . pack('N', 0);
    }

    private function queuePurge(string $queue, bool $noWait = false): string
    {
        return pack('n', 0) . $this->shortString($queue) . chr($noWait ? 1 : 0);
    }

    private function queueDelete(string $queue, bool $ifUnused = false, bool $ifEmpty = false, bool $noWait = false): string
    {
        $bits = 0;
        if ($ifUnused) {
            $bits |= 0b00000001;
        }
        if ($ifEmpty) {
            $bits |= 0b00000010;
        }
        if ($noWait) {
            $bits |= 0b00000100;
        }

        return pack('n', 0) . $this->shortString($queue) . chr($bits);
    }

    private function queueUnbind(string $queue, string $exchange, string $routingKey): string
    {
        return pack('n', 0)
            . $this->shortString($queue)
            . $this->shortString($exchange)
            . $this->shortString($routingKey)
            . pack('N', 0);
    }

    private function exchangeDelete(string $exchange, bool $ifUnused = false, bool $noWait = false): string
    {
        $bits = 0;
        if ($ifUnused) {
            $bits |= 0b00000001;
        }
        if ($noWait) {
            $bits |= 0b00000010;
        }

        return pack('n', 0) . $this->shortString($exchange) . chr($bits);
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

    private function authenticator(): Authenticator
    {
        return new Authenticator(new UserRepository($this->connection));
    }

    private function authorizer(): Authorizer
    {
        return new Authorizer(new UserRepository($this->connection));
    }

    private function tuneOk(int $heartbeat): string
    {
        return pack('nNn', 0, 131072, $heartbeat);
    }

    /**
     * @param resource $client
     * @return array{0: int, 1: int}
     */
    private function readMethod(AmqpListener $listener, mixed $client): array
    {
        return $this->readMethodFrame($listener, $client)->method();
    }

    /**
     * @param resource $client
     */
    private function readMethodFrame(AmqpListener $listener, mixed $client): Frame
    {
        $codec = new FrameCodec();

        for ($attempt = 0; $attempt < 100; $attempt++) {
            $listener->tick();
            $bytes = fread($client, 8192);
            if (is_string($bytes) && $bytes !== '') {
                $frames = $codec->push($bytes);
                if ($frames !== []) {
                    self::assertSame(Frame::TYPE_METHOD, $frames[0]->type);

                    return $frames[0];
                }
            }
            usleep(1000);
        }

        self::fail('Timed out waiting for AMQP method frame.');
    }

    private function replyCode(Frame $frame): int
    {
        $value = unpack('nreplyCode', substr($frame->payload, 4, 2));

        return (int) $value['replyCode'];
    }

    private function messageCount(Frame $frame): int
    {
        $value = unpack('NmessageCount', substr($frame->payload, 4, 4));

        return (int) $value['messageCount'];
    }

    private function assertNoFrame(AmqpListener $listener, mixed $client): void
    {
        for ($attempt = 0; $attempt < 10; $attempt++) {
            $listener->tick();
            $bytes = fread($client, 8192);
            if (is_string($bytes) && $bytes !== '') {
                $frames = (new FrameCodec())->push($bytes);
                if ($frames !== []) {
                    self::fail('Unexpected AMQP frame was received.');
                }
            }
            usleep(1000);
        }

        self::assertTrue(true);
    }

    private function createDelivery(int $destinationId, string $payload): void
    {
        $message = (new MessageRepository($this->connection))->create($payload);
        $route = (new MessageRouteRepository($this->connection))->create($message->id, $destinationId);
        $subscriptions = new SubscriptionRepository($this->connection);
        $subscription = $subscriptions->findByName($destinationId, 'amqp')
            ?? $subscriptions->create($destinationId, 'amqp');

        (new DeliveryRepository($this->connection))->create($route->id, $subscription->id);
    }

    private function deliveryStateCount(int $destinationId, string $state): int
    {
        $statement = $this->pdo->prepare('SELECT COUNT(*) FROM deliveries WHERE destination_id = :destination_id AND state = :state');
        $statement->execute(['destination_id' => $destinationId, 'state' => $state]);

        return (int) $statement->fetchColumn();
    }

    private function tableCount(string $table): int
    {
        return (int) $this->pdo->query(sprintf('SELECT count(*) FROM %s', $table))->fetchColumn();
    }

    private function broker(?ResourceLimits $limits = null): Broker
    {
        return new Broker(
            new VirtualHostRepository($this->connection),
            new PublishTransaction($this->connection, $limits ?? new ResourceLimits()),
            new DestinationRepository($this->connection),
            new SubscriptionRepository($this->connection),
            new DeliveryRepository($this->connection),
            new BindingRepository($this->connection),
            new RoutingSourceRepository($this->connection),
            limits: $limits
        );
    }

    private function resetSchema(): void
    {
        $this->pdo->exec('DROP SCHEMA public CASCADE');
        $this->pdo->exec('CREATE SCHEMA public');
    }

    private function assertSafeTestDatabase(): void
    {
        $database = (string) $this->pdo->query('SELECT current_database()')->fetchColumn();

        if (!str_contains(strtolower($database), 'test')) {
            self::markTestSkipped(sprintf(
                'Refusing to reset PostgreSQL database "%s"; FLUXQ_TEST_DATABASE_URL must point to a test database.',
                $database
            ));
        }
    }
}
