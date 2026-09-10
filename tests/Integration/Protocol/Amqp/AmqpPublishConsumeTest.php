<?php

declare(strict_types=1);

namespace FluxQ\Tests\Integration\Protocol\Amqp;

use FluxQ\Broker\Broker;
use FluxQ\Broker\Authorizer;
use FluxQ\Broker\Authenticator;
use FluxQ\Broker\DeliveryState;
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
use FluxQ\Protocol\Amqp\AmqpTlsConfig;
use FluxQ\Protocol\Amqp\Frame;
use FluxQ\Protocol\Amqp\FrameCodec;
use FluxQ\Runtime\ConnectionRegistry;
use FluxQ\Runtime\ConsumerRegistry;
use FluxQ\Tests\Fixtures\TlsCertificate;
use PDO;
use PHPUnit\Framework\Attributes\Before;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class AmqpPublishConsumeTest extends TestCase
{
    private Connection $connection;
    private PDO $pdo;
    private int $virtualHostId;

    /**
     * @var list<Frame>
     */
    private array $pendingFrames = [];

    #[Before]
    public function setUpDatabase(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            self::markTestSkipped('The pdo_pgsql extension is required for AMQP publish/consume integration tests.');
        }

        $dsn = getenv('FLUXQ_TEST_DATABASE_URL');
        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('Set FLUXQ_TEST_DATABASE_URL to run AMQP publish/consume integration tests.');
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

    public function testDefaultExchangePublishConsumeAndAckPreservesBinaryBodyAndProperties(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();
        $payload = "hello\x00world\xff";

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, strlen($payload), 'application/octet-stream', 7)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, substr($payload, 0, 5))));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, substr($payload, 5))));
            $listener->tick();

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $deliver = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $deliver->method());
            $deliveryTag = $this->deliveryTagFromDeliver($deliver);
            $header = $this->readFrame($listener, $client);
            self::assertSame(Frame::TYPE_HEADER, $header->type);
            self::assertSame(strlen($payload), $this->bodySizeFromHeader($header));
            $body = $this->readFrame($listener, $client);
            self::assertSame(Frame::TYPE_BODY, $body->type);
            self::assertSame($payload, $body->payload);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        $delivery = $this->singleDelivery('orders');
        self::assertSame(DeliveryState::Acknowledged, $delivery->state);

        $message = (new MessageRepository($this->connection))->findById(
            (new MessageRouteRepository($this->connection))->findById($delivery->messageRouteId)?->messageId ?? 0
        );
        self::assertNotNull($message);
        self::assertSame($payload, $message->payload);
        self::assertSame('application/octet-stream', $message->contentType);
        self::assertSame(7, $message->priority);
    }

    public function testAmqpBasicPropertiesRoundTripIndividuallyThroughBasicGet(): void
    {
        [$listener, $client] = $this->startedListener();

        $cases = [
            'content_type' => ['content_type' => 'application/json'],
            'content_encoding' => ['content_encoding' => 'utf-8'],
            'headers' => ['headers' => ['string' => 'value', 'int' => 27, 'bool' => true]],
            'delivery_mode' => ['delivery_mode' => 1],
            'priority' => ['priority' => 5],
            'correlation_id' => ['correlation_id' => 'correlation-0027'],
            'reply_to' => ['reply_to' => 'reply.queue'],
            'expiration' => ['expiration' => '60000'],
            'message_id' => ['message_id' => 'message-0027'],
            'timestamp' => ['timestamp' => 1777777777],
            'type' => ['type' => 'fluxq.test.message'],
            'user_id' => ['user_id' => 'guest'],
            'app_id' => ['app_id' => 'fluxq-pika-test'],
            'cluster_id' => ['cluster_id' => 'cluster-a'],
        ];

        try {
            $this->connectAndOpenChannel($listener, $client, 1);

            foreach ($cases as $name => $properties) {
                $queue = 'props.' . $name;
                $body = 'body-' . $name;
                $this->sendMethod($client, 1, 50, 10, $this->queueDeclare($queue));
                self::assertSame([50, 11], $this->readMethod($listener, $client));
                $this->publishBodyWithProperties($listener, $client, 1, '', $queue, $body, $properties);

                $this->sendMethod($client, 1, 60, 70, $this->basicGet($queue, noAck: true));
                self::assertSame([60, 71], $this->readMethodFrame($listener, $client)->method());
                $header = $this->readFrame($listener, $client);
                self::assertEquals($properties, $this->basicPropertiesFromHeader($header));
                self::assertSame($body, $this->readFrame($listener, $client)->payload);
            }
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testAmqpBasicPropertiesRoundTripThroughBasicGetAndKeepInternalIdSeparate(): void
    {
        [$listener, $client] = $this->startedListener();
        $properties = $this->completeBasicProperties(1777777777);
        $payload = "{\"id\":27}\x00\xff";

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('properties.get'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->publishBodyWithProperties($listener, $client, 1, '', 'properties.get', $payload, $properties);

            $message = $this->messageForSingleDelivery('properties.get');
            self::assertNotSame('message-0027', $message->messageId);
            self::assertMatchesRegularExpression(
                '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
                $message->messageId
            );

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('properties.get', noAck: true));
            self::assertSame([60, 71], $this->readMethodFrame($listener, $client)->method());
            $header = $this->readFrame($listener, $client);
            self::assertEquals($properties, $this->basicPropertiesFromHeader($header));
            self::assertSame($payload, $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testAmqpBasicPropertiesRoundTripThroughBasicConsumeAfterRestart(): void
    {
        [$listener, $client] = $this->startedListener();
        $properties = $this->completeBasicProperties(1777777778);
        $payload = "binary\x00body\xff";

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('properties.consume'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->publishBodyWithProperties($listener, $client, 1, '', 'properties.consume', $payload, $properties);
        } finally {
            fclose($client);
            $listener->stop();
        }

        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('properties.consume', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame([60, 60], $this->readMethodFrame($listener, $client)->method());
            $header = $this->readFrame($listener, $client);
            self::assertEquals($properties, $this->basicPropertiesFromHeader($header));
            self::assertSame($payload, $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testOmittedAmqpBasicPropertiesRemainAbsentOnDelivery(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('properties.omitted'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->publishBodyWithProperties($listener, $client, 1, '', 'properties.omitted', 'payload', []);

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('properties.omitted', noAck: true));
            self::assertSame([60, 71], $this->readMethodFrame($listener, $client)->method());
            $header = $this->readFrame($listener, $client);
            self::assertSame([], $this->basicPropertiesFromHeader($header));
            self::assertSame('payload', $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testExclusiveQueueLifecycleIsScopedToOwningConnection(): void
    {
        [$listener, $owner] = $this->startedListener();
        $second = null;
        $third = null;

        try {
            $this->connectAndOpenChannel($listener, $owner, 1);
            $this->sendMethod($owner, 1, 50, 10, $this->queueDeclare(
                'exclusive.orders',
                durable: false,
                exclusive: true,
                autoDelete: false
            ));
            self::assertSame([50, 11], $this->readMethod($listener, $owner));

            $this->publishBody($listener, $owner, 1, '', 'exclusive.orders', 'owned-message');

            $this->sendMethod($owner, 1, 50, 10, $this->queueDeclare(
                'exclusive.orders',
                passive: true,
                durable: false,
                exclusive: true
            ));
            self::assertSame([50, 11], $this->readMethod($listener, $owner));

            $second = $this->connectClient($listener, 1);
            $this->sendMethod($second, 1, 50, 10, $this->queueDeclare(
                'exclusive.orders',
                passive: true,
                durable: false
            ));
            $locked = $this->readMethodFrame($listener, $second);
            self::assertSame([20, 40], $locked->method());
            self::assertSame(405, $this->replyCode($locked));

            $this->sendMethod($second, 2, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 2, 60, 20, $this->basicConsume('exclusive.orders', 'blocked-consumer'));
            $locked = $this->readMethodFrame($listener, $second);
            self::assertSame([20, 40], $locked->method());
            self::assertSame(405, $this->replyCode($locked));

            $this->sendMethod($second, 3, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 3, 60, 70, $this->basicGet('exclusive.orders', noAck: true));
            $locked = $this->readMethodFrame($listener, $second);
            self::assertSame([20, 40], $locked->method());
            self::assertSame(405, $this->replyCode($locked));

            $this->sendMethod($second, 4, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 4, 50, 30, $this->queuePurge('exclusive.orders'));
            $locked = $this->readMethodFrame($listener, $second);
            self::assertSame([20, 40], $locked->method());
            self::assertSame(405, $this->replyCode($locked));

            $this->sendMethod($second, 5, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 5, 50, 40, $this->queueDelete('exclusive.orders'));
            $locked = $this->readMethodFrame($listener, $second);
            self::assertSame([20, 40], $locked->method());
            self::assertSame(405, $this->replyCode($locked));

            $this->sendMethod($second, 6, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 6, 40, 10, $this->exchangeDeclare('blocked.direct', 'direct'));
            self::assertSame([40, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 6, 50, 20, $this->queueBind('exclusive.orders', 'blocked.direct', 'key'));
            $locked = $this->readMethodFrame($listener, $second);
            self::assertSame([20, 40], $locked->method());
            self::assertSame(405, $this->replyCode($locked));

            $this->sendMethod($second, 7, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 7, 50, 10, $this->queueDeclare('public.orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $second));

            $this->sendMethod($owner, 1, 60, 70, $this->basicGet('exclusive.orders', noAck: true));
            self::assertSame([60, 71], $this->readMethod($listener, $owner));
            $header = $this->readFrame($listener, $owner);
            self::assertSame(Frame::TYPE_HEADER, $header->type);
            self::assertSame(13, $this->bodySizeFromHeader($header));
            $body = $this->readFrame($listener, $owner);
            self::assertSame(Frame::TYPE_BODY, $body->type);
            self::assertSame('owned-message', $body->payload);

            $this->sendMethod($owner, 0, 10, 50, pack('n', 200) . $this->shortString('') . pack('nn', 0, 0));
            self::assertSame([10, 51], $this->readMethod($listener, $owner));
            $this->awaitEof($listener, $owner);

            self::assertNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'exclusive.orders'));
            self::assertSame(1, $this->tableCount('messages'));

            $third = $this->connectClient($listener, 1);
            $this->sendMethod($third, 1, 50, 10, $this->queueDeclare(
                'exclusive.orders',
                passive: true,
                durable: false
            ));
            $missing = $this->readMethodFrame($listener, $third);
            self::assertSame([20, 40], $missing->method());
            self::assertSame(404, $this->replyCode($missing));
        } finally {
            foreach ([$owner, $second, $third] as $client) {
                if (is_resource($client)) {
                    fclose($client);
                }
            }
            $listener->stop();
        }
    }

    public function testExclusiveQueueDoesNotSurviveRuntimeRestart(): void
    {
        $broker = $this->broker();
        $broker->declareQueue(
            '/',
            'stale.exclusive',
            durable: false,
            autoDelete: false,
            exclusive: true,
            connectionId: '00000000-0000-4000-8000-000000000001'
        );
        self::assertNotNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'stale.exclusive'));

        [$listener, $client] = $this->startedListener();

        try {
            self::assertNull((new DestinationRepository($this->connection))->findByName($this->virtualHostId, 'stale.exclusive'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testServerNamedExclusiveAutoDeleteQueuesUseGeneratedNamesAndOwnerLifecycle(): void
    {
        [$listener, $owner] = $this->startedListener();
        $second = null;
        $third = null;

        try {
            $this->connectAndOpenChannel($listener, $owner, 1);
            $this->sendMethod($owner, 1, 50, 10, $this->queueDeclare(
                '',
                durable: false,
                exclusive: true,
                autoDelete: true
            ));
            $firstDeclareOk = $this->readMethodFrame($listener, $owner);
            self::assertSame([50, 11], $firstDeclareOk->method());
            $firstQueue = $this->queueDeclareName($firstDeclareOk);
            self::assertNotSame('', $firstQueue);
            self::assertStringStartsWith('amq.gen-', $firstQueue);

            $this->publishBody($listener, $owner, 1, '', $firstQueue, 'first');
            $this->sendMethod($owner, 1, 50, 10, $this->queueDeclare($firstQueue, passive: true, durable: false));
            $firstPassiveOk = $this->readMethodFrame($listener, $owner);
            self::assertSame([50, 11], $firstPassiveOk->method());
            self::assertSame($firstQueue, $this->queueDeclareName($firstPassiveOk));
            self::assertSame(1, $this->queueDeclareMessageCount($firstPassiveOk));

            $this->sendMethod($owner, 1, 60, 70, $this->basicGet($firstQueue, noAck: true));
            self::assertSame([60, 71], $this->readMethod($listener, $owner));
            self::assertSame(Frame::TYPE_HEADER, $this->readFrame($listener, $owner)->type);
            self::assertSame('first', $this->readFrame($listener, $owner)->payload);

            $this->sendMethod($owner, 1, 50, 10, $this->queueDeclare(
                '',
                durable: false,
                exclusive: true,
                autoDelete: true
            ));
            $secondDeclareOk = $this->readMethodFrame($listener, $owner);
            self::assertSame([50, 11], $secondDeclareOk->method());
            $secondQueue = $this->queueDeclareName($secondDeclareOk);
            self::assertNotSame('', $secondQueue);
            self::assertNotSame($firstQueue, $secondQueue);

            $this->publishBody($listener, $owner, 1, '', $secondQueue, 'second');
            $this->sendMethod($owner, 1, 60, 20, $this->basicConsume($secondQueue, 'generated-consumer'));
            self::assertSame([60, 21], $this->readMethod($listener, $owner));
            self::assertSame('second', $this->readDeliveryBody($listener, $owner));

            $second = $this->connectClient($listener, 1);
            $this->sendMethod($second, 1, 50, 10, $this->queueDeclare($firstQueue, passive: true, durable: false));
            $locked = $this->readMethodFrame($listener, $second);
            self::assertSame([20, 40], $locked->method());
            self::assertSame(405, $this->replyCode($locked));

            $this->sendMethod($second, 2, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $second));
            $this->sendMethod($second, 2, 50, 10, $this->queueDeclare('public.after-lock'));
            self::assertSame([50, 11], $this->readMethod($listener, $second));

            $this->sendMethod($owner, 0, 10, 50, pack('n', 200) . $this->shortString('') . pack('nn', 0, 0));
            self::assertSame([10, 51], $this->readMethod($listener, $owner));
            $this->awaitEof($listener, $owner);

            self::assertFalse($this->queueExists($firstQueue));
            self::assertFalse($this->queueExists($secondQueue));

            $third = $this->connectClient($listener, 1);
            $this->sendMethod($third, 1, 50, 10, $this->queueDeclare($firstQueue, passive: true, durable: false));
            $missingFirst = $this->readMethodFrame($listener, $third);
            self::assertSame([20, 40], $missingFirst->method());
            self::assertSame(404, $this->replyCode($missingFirst));

            $this->sendMethod($third, 2, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $third));
            $this->sendMethod($third, 2, 50, 10, $this->queueDeclare($secondQueue, passive: true, durable: false));
            $missingSecond = $this->readMethodFrame($listener, $third);
            self::assertSame([20, 40], $missingSecond->method());
            self::assertSame(404, $this->replyCode($missingSecond));
        } finally {
            foreach ([$owner, $second, $third] as $client) {
                if (is_resource($client)) {
                    fclose($client);
                }
            }
            $listener->stop();
        }
    }

    public function testQueueDeclareOkReportsZeroMessagesForEmptyQueue(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(0, $this->queueDeclareMessageCount($declareOk));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeclareOkReportsOneReadyMessageAfterPublish(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->publishBody($listener, $client, 1, '', 'orders', 'one');

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(1, $this->queueDeclareMessageCount($declareOk));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeclareOkReportsSeveralReadyMessagesAfterPublishes(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three']);

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(3, $this->queueDeclareMessageCount($declareOk));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeclareOkMessageCountDecreasesAfterBasicGetNoAck(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two']);

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders', noAck: true));
            $getOk = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 71], $getOk->method());
            $this->readFrame($listener, $client);
            self::assertSame('one', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(1, $this->queueDeclareMessageCount($declareOk));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPassiveQueueDeclareOkReportsReadyMessageCount(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two']);

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(2, $this->queueDeclareMessageCount($declareOk));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPassiveDurableQueueDeclareReportsPersistentMessageDepthBeforeAndAfterAcknowledgement(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('test_durable_queue', durable: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            foreach (['one', 'two', 'three'] as $body) {
                $this->publishBody($listener, $client, 1, '', 'test_durable_queue', $body);
            }

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('test_durable_queue', passive: true, durable: false));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(3, $this->queueDeclareMessageCount($declareOk));

            for ($i = 0; $i < 3; $i++) {
                $this->sendMethod($client, 1, 60, 70, $this->basicGet('test_durable_queue'));
                $getOk = $this->readMethodFrame($listener, $client);
                self::assertSame([60, 71], $getOk->method());
                $deliveryTag = $this->deliveryTagFromBasicGetOk($getOk);
                self::assertSame(Frame::TYPE_HEADER, $this->readFrame($listener, $client)->type);
                self::assertSame(Frame::TYPE_BODY, $this->readFrame($listener, $client)->type);
                $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
                $listener->tick();
            }

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('test_durable_queue', passive: true, durable: false));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(0, $this->queueDeclareMessageCount($declareOk));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPassiveQueueDeclareOkReportsConsumerCount(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $declareOk = $this->readMethodFrame($listener, $client);

            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(1, $this->queueDeclareConsumerCount($declareOk));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testRoutableMandatoryPublishWithConfirmsIsAcknowledged(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('orders.direct'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders', 'orders.direct', 'created'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'orders.direct', 'created', 'routable', mandatory: true);
            $ack = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->tableCount('message_routes'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testFanoutPublishIgnoresRoutingKeyAndDeliversToEveryBoundQueue(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            foreach (['orders-a', 'orders-b', 'unbound'] as $queue) {
                $this->sendMethod($client, 1, 50, 10, $this->queueDeclare($queue));
                self::assertSame([50, 11], $this->readMethod($listener, $client));
            }
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('events', 'fanout'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders-a', 'events', 'created'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders-b', 'events', 'updated'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'events', 'ignored', 'fanout-body', mandatory: true);
            $ack = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(2, $this->tableCount('message_routes'));
            self::assertSame(2, $this->tableCount('deliveries'));

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders-a', noAck: true));
            self::assertSame([60, 71], $this->readMethod($listener, $client));
            self::assertSame(Frame::TYPE_HEADER, $this->readFrame($listener, $client)->type);
            self::assertSame('fanout-body', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders-b', noAck: true));
            self::assertSame([60, 71], $this->readMethod($listener, $client));
            self::assertSame(Frame::TYPE_HEADER, $this->readFrame($listener, $client)->type);
            self::assertSame('fanout-body', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('unbound', noAck: true));
            self::assertSame([60, 72], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testFanoutPublishResourceFailureRollsBackAndDoesNotConfirm(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxQueueDepth: 1));

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            foreach (['orders-a', 'orders-b'] as $queue) {
                $this->sendMethod($client, 1, 50, 10, $this->queueDeclare($queue));
                self::assertSame([50, 11], $this->readMethod($listener, $client));
            }
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('events', 'fanout'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders-a', 'events', 'a'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders-b', 'events', 'b'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, '', 'orders-b', 'already-full');
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->tableCount('message_routes'));
            self::assertSame(1, $this->tableCount('deliveries'));

            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'events', 'ignored', 'blocked', mandatory: true);
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(506, $this->replyCode($close));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->tableCount('message_routes'));
            self::assertSame(1, $this->tableCount('deliveries'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testTopicPublishRoutesToMatchingQueuesAndSupportsGetAndConsume(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            foreach (['orders-one', 'orders-all', 'failed', 'unbound'] as $queue) {
                $this->sendMethod($client, 1, 50, 10, $this->queueDeclare($queue));
                self::assertSame([50, 11], $this->readMethod($listener, $client));
            }
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('events.topic', 'topic'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders-one', 'events.topic', 'orders.*'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders-all', 'events.topic', 'orders.#'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('failed', 'events.topic', '*.failed'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'events.topic', 'orders.failed', 'topic-body', mandatory: true);
            $ack = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(3, $this->tableCount('message_routes'));
            self::assertSame(3, $this->tableCount('deliveries'));

            foreach (['orders-one', 'orders-all', 'failed'] as $queue) {
                $this->sendMethod($client, 1, 60, 70, $this->basicGet($queue, noAck: true));
                self::assertSame([60, 71], $this->readMethod($listener, $client));
                self::assertSame(Frame::TYPE_HEADER, $this->readFrame($listener, $client)->type);
                self::assertSame('topic-body', $this->readFrame($listener, $client)->payload);
            }

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('unbound', noAck: true));
            self::assertSame([60, 72], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'events.topic', 'orders.created', 'consume-body');
            self::assertSame(2, $this->deliveryTagFromBasicAck($this->readMethodFrame($listener, $client)));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders-one', 'topic-consumer'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame('consume-body', $this->readDeliveryBody($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedTopicPublishClosesChannelWithAccessRefused(): void
    {
        $broker = $this->broker();
        $broker->declareQueue('/', 'orders', true, false);
        $broker->declareTopicRoutingSource('/', 'events.topic', true, false);
        $broker->bindQueue('/', 'events.topic', 'orders', 'orders.#');
        (new UserRepository($this->connection))->setPermissions('guest', '/', '.*', '^allowed$', '.*');
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('events.topic', 'orders.created'));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
            self::assertSame(0, $this->tableCount('messages'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testTopicPublishResourceFailureRollsBackAndDoesNotConfirm(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxQueueDepth: 1));

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            foreach (['orders', 'audit'] as $queue) {
                $this->sendMethod($client, 1, 50, 10, $this->queueDeclare($queue));
                self::assertSame([50, 11], $this->readMethod($listener, $client));
            }
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('events.topic', 'topic'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders', 'events.topic', 'orders.*'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('audit', 'events.topic', '#.failed'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, '', 'audit', 'already-full');
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->tableCount('message_routes'));
            self::assertSame(1, $this->tableCount('deliveries'));

            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'events.topic', 'orders.failed', 'blocked', mandatory: true);
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(506, $this->replyCode($close));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $this->tableCount('message_routes'));
            self::assertSame(1, $this->tableCount('deliveries'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testUnroutableMandatoryPublishReturnsOriginalMessageAndThenConfirms(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('orders.direct'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'orders.direct', 'missing', 'returned', mandatory: true);
            $return = $this->readMethodFrame($listener, $client);
            $header = $this->readFrame($listener, $client);
            $body = $this->readFrame($listener, $client);
            $ack = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 50], $return->method());
            self::assertSame(
                [
                    'reply_code' => 312,
                    'exchange' => 'orders.direct',
                    'routing_key' => 'missing',
                ],
                $this->basicReturnDetails($return)
            );
            self::assertStringContainsString('NO_ROUTE', $this->basicReturnReplyText($return));
            self::assertSame(Frame::TYPE_HEADER, $header->type);
            self::assertSame(strlen('returned'), $this->bodySizeFromHeader($header));
            self::assertSame(Frame::TYPE_BODY, $body->type);
            self::assertSame('returned', $body->payload);
            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertSame(0, $this->tableCount('messages'));
            self::assertSame(0, $this->tableCount('message_routes'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testUnroutableNonMandatoryPublishIsSilentlyDiscardedAndConfirmed(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('orders.direct'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, 'orders.direct', 'missing', 'discarded');
            $ack = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertSame(0, $this->tableCount('messages'));
            self::assertSame(0, $this->tableCount('message_routes'));
            self::assertSame(0, $this->tableCount('deliveries'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testTlsDefaultExchangePublishConsumeAndAckUsesSameBrokerFlow(): void
    {
        [$listener, $client] = $this->startedTlsListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, 5)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, 'hello')));
            $listener->tick();

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $deliver = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $deliver->method());
            $deliveryTag = $this->deliveryTagFromDeliver($deliver);
            $this->readFrame($listener, $client);
            $body = $this->readFrame($listener, $client);
            self::assertSame('hello', $body->payload);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Acknowledged, $this->singleDelivery('orders')->state);
    }

    public function testInvalidCredentialsNeverReceiveOpenOkAndCleanRuntimeConnection(): void
    {
        $connections = new ConnectionRegistry();
        [$listener, $client] = $this->startedListener($connections);

        try {
            fwrite($client, AmqpConnection::PROTOCOL_HEADER);
            self::assertSame([10, 10], $this->readMethod($listener, $client));
            $this->sendMethod($client, 0, 10, 11, $this->startOk("\0guest\0wrong"));

            self::assertSame([10, 50], $this->readMethod($listener, $client));
            self::assertSame(0, $connections->count());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testUnknownVirtualHostNeverReceivesOpenOkAndCleanRuntimeConnection(): void
    {
        $connections = new ConnectionRegistry();
        [$listener, $client] = $this->startedListener($connections);

        try {
            fwrite($client, AmqpConnection::PROTOCOL_HEADER);
            self::assertSame([10, 10], $this->readMethod($listener, $client));
            $this->sendMethod($client, 0, 10, 11, $this->startOk());
            self::assertSame([10, 30], $this->readMethod($listener, $client));
            $this->sendMethod($client, 0, 10, 31, $this->tuneOk(60));
            $listener->tick();
            $this->sendMethod($client, 0, 10, 40, $this->connectionOpen('missing'));

            self::assertSame([10, 50], $this->readMethod($listener, $client));
            self::assertSame(0, $connections->count());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedPublishClosesChannelWithAccessRefused(): void
    {
        (new UserRepository($this->connection))->setPermissions('guest', '/', '.*', '^allowed$', '.*');
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('orders.direct', 'created'));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedConsumeClosesChannelWithAccessRefused(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        (new UserRepository($this->connection))->setPermissions('guest', '/', '.*', '.*', '^allowed$');
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDeniedBasicGetClosesChannelWithAccessRefused(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        (new UserRepository($this->connection))->setPermissions('guest', '/', '.*', '.*', '^allowed$');
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders'));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testAuthorizationStillAppliesOverTls(): void
    {
        $this->broker()->declareQueue('/', 'orders', true, false);
        (new UserRepository($this->connection))->setPermissions('guest', '/', '.*', '.*', '^allowed$');
        [$listener, $client] = $this->startedTlsListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(403, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testTlsDisconnectReleasesUnackedDelivery(): void
    {
        [$listener, $client] = $this->startedTlsListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame([60, 60], $this->readMethod($listener, $client));
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);
            fclose($client);

            for ($attempt = 0; $attempt < 50; $attempt++) {
                $listener->tick();
                usleep(1000);
            }
        } finally {
            $listener->stop();
        }

        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testDirectExchangeRejectWithRequeueRedeliversThenRejects(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 40, 10, $this->exchangeDeclare('orders.direct'));
            self::assertSame([40, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 20, $this->queueBind('orders', 'orders.direct', 'created'));
            self::assertSame([50, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('orders.direct', 'created'));
            fwrite($client, $codec->encode($this->contentHeader(1, 5)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, 'first')));
            $listener->tick();

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $first = $this->readMethodFrame($listener, $client);
            $firstTag = $this->deliveryTagFromDeliver($first);
            $this->readFrame($listener, $client);
            self::assertSame('first', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 90, $this->packLongLong($firstTag) . "\x01");
            $redelivered = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $redelivered->method());
            $secondTag = $this->deliveryTagFromDeliver($redelivered);
            self::assertNotSame($firstTag, $secondTag);
            $this->readFrame($listener, $client);
            self::assertSame('first', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 90, $this->packLongLong($secondTag) . "\x00");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Rejected, $this->singleDelivery('orders')->state);
    }

    public function testBasicGetReturnsAvailableMessageWithBinaryBodyAndManualAck(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $payload = "get\x00binary\xff";
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', [$payload]);

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders'));
            $get = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 71], $get->method());
            self::assertSame(1, $this->deliveryTagFromBasicGetOk($get));
            $header = $this->readFrame($listener, $client);
            self::assertSame(strlen($payload), $this->bodySizeFromHeader($header));
            self::assertSame($payload, $this->readFrame($listener, $client)->payload);
            self::assertSame(DeliveryState::Reserved, $this->singleDelivery('orders')->state);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(1) . "\x00");
            $listener->tick();
            self::assertSame(DeliveryState::Acknowledged, $this->singleDelivery('orders')->state);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testBasicAckMultipleAcknowledgesCumulativeBasicGetDeliveries(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three', 'four', 'five']);

            $tags = [];
            for ($i = 0; $i < 3; $i++) {
                [$tag, $redelivered, $body] = $this->readBasicGetDelivery($listener, $client, 'orders');
                $tags[] = $tag;
                self::assertFalse($redelivered);
                self::assertSame(['one', 'two', 'three'][$i], $body);
            }

            self::assertSame([1, 2, 3], $tags);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(3) . "\x01");
            $listener->tick();
            self::assertSame(0, $listener->inFlightCount());

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $declareOk = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(2, $this->queueDeclareMessageCount($declareOk));

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders', noAck: true));
            $next = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 71], $next->method());
            self::assertSame(4, $this->deliveryTagFromBasicGetOk($next));
            $this->readFrame($listener, $client);
            self::assertSame('four', $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
                DeliveryState::Pending,
            ],
            $this->deliveryStates('orders')
        );
    }

    public function testBasicAckMultipleZeroAcknowledgesAllOutstandingAndFreesPrefetch(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            self::assertSame(1, $firstTag);
            self::assertSame('one', $firstBody);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(0) . "\x01");
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            self::assertSame(2, $secondTag);
            self::assertSame('two', $secondBody);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($secondTag) . "\x00");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Acknowledged, DeliveryState::Acknowledged], $this->deliveryStates('orders'));
    }

    public function testBasicAckMultipleKeepsHigherTagsOutstandingAndDoesNotResettleRemovedTags(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three', 'four']);

            for ($i = 0; $i < 4; $i++) {
                $this->readBasicGetDelivery($listener, $client, 'orders');
            }

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(2) . "\x01");
            $listener->tick();
            self::assertSame(
                [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Reserved, DeliveryState::Reserved],
                $this->deliveryStates('orders')
            );

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(4) . "\x01");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged],
            $this->deliveryStates('orders')
        );
    }

    public function testBasicNackMultipleWithRequeueReleasesAllAffectedDeliveriesForRedelivery(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three']);

            for ($i = 0; $i < 3; $i++) {
                $this->readBasicGetDelivery($listener, $client, 'orders');
            }

            $this->sendMethod($client, 1, 60, 120, $this->basicNack(3, true, true));
            $listener->tick();
            self::assertSame(0, $listener->inFlightCount());

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $declareOk = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 11], $declareOk->method());
            self::assertSame(3, $this->queueDeclareMessageCount($declareOk));

            foreach (['one', 'two', 'three'] as $expectedBody) {
                [$tag, $redelivered, $body] = $this->readBasicGetDelivery($listener, $client, 'orders', noAck: true);
                self::assertGreaterThan(3, $tag);
                self::assertTrue($redelivered);
                self::assertSame($expectedBody, $body);
            }
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged], $this->deliveryStates('orders'));
    }

    public function testBasicNackMultipleWithRejectRejectsAllAffectedDeliveries(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three']);

            for ($i = 0; $i < 3; $i++) {
                $this->readBasicGetDelivery($listener, $client, 'orders');
            }

            $this->sendMethod($client, 1, 60, 120, $this->basicNack(3, true, false));
            $listener->tick();

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders'));
            self::assertSame([60, 72], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Rejected, DeliveryState::Rejected, DeliveryState::Rejected], $this->deliveryStates('orders'));
    }

    public function testCumulativeDeliveryTagsAreChannelLocal(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 2, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two']);

            [$channelOneTag] = $this->readBasicGetDelivery($listener, $client, 'orders', channel: 1);
            [$channelTwoTag] = $this->readBasicGetDelivery($listener, $client, 'orders', channel: 2);
            self::assertSame(1, $channelOneTag);
            self::assertSame(1, $channelTwoTag);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(1) . "\x01");
            $listener->tick();
            self::assertSame([DeliveryState::Acknowledged, DeliveryState::Reserved], $this->deliveryStates('orders'));

            $this->sendMethod($client, 2, 60, 80, $this->packLongLong(1) . "\x01");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Acknowledged, DeliveryState::Acknowledged], $this->deliveryStates('orders'));
    }

    public function testInvalidCumulativeAckTagClosesChannelWithPreconditionFailed(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two']);
            $this->readBasicGetDelivery($listener, $client, 'orders');
            $this->readBasicGetDelivery($listener, $client, 'orders');

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(3) . "\x01");
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Pending, DeliveryState::Pending], $this->deliveryStates('orders'));
    }

    public function testCumulativeAckNackEndToEndRegression(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['a1', 'a2', 'a3', 'ready-1', 'ready-2']);

            for ($i = 0; $i < 3; $i++) {
                $this->readBasicGetDelivery($listener, $client, 'orders');
            }
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong(3) . "\x01");
            $listener->tick();
            $this->assertReadyMessageCount($listener, $client, 'orders', 2);

            $this->declareQueueAndPublishBodies($listener, $client, 'requeue.orders', ['r1', 'r2', 'r3']);
            $latestTag = 0;
            for ($i = 0; $i < 3; $i++) {
                [$latestTag] = $this->readBasicGetDelivery($listener, $client, 'requeue.orders');
            }
            $this->sendMethod($client, 1, 60, 120, $this->basicNack($latestTag, true, true));
            $listener->tick();
            $this->assertReadyMessageCount($listener, $client, 'requeue.orders', 3);
            foreach (['r1', 'r2', 'r3'] as $body) {
                [, $redelivered, $redeliveredBody] = $this->readBasicGetDelivery($listener, $client, 'requeue.orders', noAck: true);
                self::assertTrue($redelivered);
                self::assertSame($body, $redeliveredBody);
            }

            $this->declareQueueAndPublishBodies($listener, $client, 'reject.orders', ['x1', 'x2', 'x3']);
            $latestTag = 0;
            for ($i = 0; $i < 3; $i++) {
                [$latestTag] = $this->readBasicGetDelivery($listener, $client, 'reject.orders');
            }
            $this->sendMethod($client, 1, 60, 120, $this->basicNack($latestTag, true, false));
            $listener->tick();
            $this->sendMethod($client, 1, 60, 70, $this->basicGet('reject.orders'));
            self::assertSame([60, 72], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Pending, DeliveryState::Pending],
            $this->deliveryStates('orders')
        );
        self::assertSame([DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged], $this->deliveryStates('requeue.orders'));
        self::assertSame([DeliveryState::Rejected, DeliveryState::Rejected, DeliveryState::Rejected], $this->deliveryStates('reject.orders'));
    }

    public function testBasicGetEmptyWhenNoMessageIsAvailable(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders'));

            self::assertSame([60, 72], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testBasicGetNoAckSettlesDeliveryImmediately(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['auto']);

            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders', true));
            self::assertSame([60, 71], $this->readMethod($listener, $client));
            $this->readFrame($listener, $client);
            self::assertSame('auto', $this->readFrame($listener, $client)->payload);
            self::assertSame(DeliveryState::Acknowledged, $this->singleDelivery('orders')->state);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeleteIfUnusedRejectsQueueWithActiveConsumerAndRuntimeContinues(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 50, 40, $this->queueDelete('orders', ifUnused: true));
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(406, $this->replyCode($close));
            self::assertSame(1, $listener->connectionCount());

            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeleteFromAnotherChannelSendsServerBasicCancelAndKeepsConnectionUsable(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(3, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 2, '', 'orders', 'done');
            $deliver = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $deliver->method());
            self::assertSame(1, $deliver->channel);
            $deliveryTag = $this->deliveryTagFromDeliver($deliver);
            $this->readFrame($listener, $client);
            self::assertSame('done', $this->readFrame($listener, $client)->payload);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 2, 50, 40, $this->queueDelete('orders'));
            $cancel = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 30], $cancel->method());
            self::assertSame(1, $cancel->channel);
            self::assertSame('consumer-a', $this->consumerTagFromBasicCancel($cancel));
            $deleteOk = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 41], $deleteOk->method());
            self::assertSame(2, $deleteOk->channel);
            self::assertSame(0, $consumers->count());
            $this->assertNoFrame($listener, $client);

            $this->sendMethod($client, 3, 50, 10, $this->queueDeclare('orders', passive: true));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(3, $close->channel);
            self::assertSame(404, $this->replyCode($close));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders.after.consumer'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 50, 10, $this->queueDeclare('orders.after.control'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            self::assertSame(1, $listener->connectionCount());
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertFalse($this->queueExists('orders'));
    }

    public function testQueueDeleteSendsOneServerBasicCancelToEachAffectedConsumerOnly(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            for ($channel = 2; $channel <= 5; $channel++) {
                fwrite($client, $codec->encode(Frame::methodFrame($channel, 20, 10, "\x00")));
                self::assertSame([20, 11], $this->readMethod($listener, $client));
            }

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('invoices'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 3, 60, 20, $this->basicConsume('orders', 'consumer-c'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 4, 60, 20, $this->basicConsume('invoices', 'consumer-other'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame(4, $consumers->count());

            $this->sendMethod($client, 5, 50, 40, $this->queueDelete('orders'));

            $cancellations = [];
            for ($i = 0; $i < 3; $i++) {
                $cancel = $this->readMethodFrame($listener, $client);
                self::assertSame([60, 30], $cancel->method());
                $cancellations[$this->consumerTagFromBasicCancel($cancel)] = $cancel->channel;
            }

            self::assertSame(
                ['consumer-a' => 1, 'consumer-b' => 2, 'consumer-c' => 3],
                $cancellations
            );
            $deleteOk = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 41], $deleteOk->method());
            self::assertSame(5, $deleteOk->channel);
            self::assertSame(1, $consumers->count());
            $this->assertNoFrame($listener, $client);

            $this->publishBody($listener, $client, 5, '', 'invoices', 'still-active');
            $delivery = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $delivery->method());
            self::assertSame(4, $delivery->channel);
            self::assertSame('consumer-other', $this->consumerTagFromDeliver($delivery));
            $this->readFrame($listener, $client);
            self::assertSame('still-active', $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testQueueDeleteWithUnackedConsumerDeliveryCancelsConsumerAndRuntimeStaysHealthy(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(3, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));

            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['held']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $deliver = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $deliver->method());
            self::assertSame(1, $deliver->channel);
            $this->readFrame($listener, $client);
            self::assertSame('held', $this->readFrame($listener, $client)->payload);
            self::assertSame(DeliveryState::Reserved, $this->singleDelivery('orders')->state);

            $this->sendMethod($client, 2, 50, 40, $this->queueDelete('orders'));
            $cancel = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 30], $cancel->method());
            self::assertSame(1, $cancel->channel);
            self::assertSame('consumer-a', $this->consumerTagFromBasicCancel($cancel));
            $deleteOk = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 41], $deleteOk->method());
            self::assertSame(2, $deleteOk->channel);
            self::assertSame(0, $consumers->count());
            self::assertFalse($this->queueExists('orders'));
            self::assertSame(1, $listener->connectionCount());

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders.after.unacked.consumer'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 50, 10, $this->queueDeclare('orders.after.unacked.control'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 3, 50, 10, $this->queueDeclare('orders', passive: true));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(3, $close->channel);
            self::assertSame(404, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testExistingAckSucceedsDuringDrainAndCompletesInflightWork(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['held']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $delivery = $this->readMethodFrame($listener, $client);
            $deliveryTag = $this->deliveryTagFromDeliver($delivery);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);

            $listener->beginDrain();
            self::assertSame(1, $listener->inFlightCount());
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            $listener->tick();

            self::assertSame(0, $listener->inFlightCount());
            self::assertSame(DeliveryState::Acknowledged, $this->singleDelivery('orders')->state);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testNewPublishConsumerAndGetAreRefusedDuringDrain(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(3, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(4, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $listener->beginDrain();

            $this->sendMethod($client, 2, 60, 40, $this->basicPublish('', 'orders'));
            $publishClose = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $publishClose->method());
            self::assertSame(320, $this->replyCode($publishClose));

            $this->sendMethod($client, 3, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            $consumeClose = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $consumeClose->method());
            self::assertSame(320, $this->replyCode($consumeClose));

            $this->sendMethod($client, 4, 60, 70, $this->basicGet('orders'));
            $getClose = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $getClose->method());
            self::assertSame(320, $this->replyCode($getClose));
            self::assertSame(1, $listener->connectionCount());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testExistingConsumersDoNotReserveNewDeliveriesDuringDrain(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $listener->beginDrain();
            $this->broker()->publishToDefaultExchange('/', 'orders', 'pending-after-drain');
            $this->assertNoFrame($listener, $client);

            self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testShutdownCleanupReleasesRemainingUnackedDeliveryAfterDrain(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['held']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->readMethodFrame($listener, $client);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);

            $listener->beginDrain();
            $listener->stop();

            self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
        } finally {
            if (is_resource($client)) {
                fclose($client);
            }
            $listener->stop();
        }
    }

    public function testReleasedDeliveryCanBeConsumedAfterRestart(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['restart']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->readMethodFrame($listener, $client);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);
            $listener->beginDrain();
            $listener->stop();
            fclose($client);
        } finally {
            $listener->stop();
        }

        [$nextListener, $nextClient] = $this->startedListener();
        try {
            $this->connectAndOpenChannel($nextListener, $nextClient, 1);
            $this->sendMethod($nextClient, 1, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($nextListener, $nextClient));

            self::assertSame('restart', $this->readDeliveryBody($nextListener, $nextClient));
        } finally {
            fclose($nextClient);
            $nextListener->stop();
        }
    }

    public function testDisconnectReleasesUnackedDeliveries(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, 4)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, 'held')));
            $listener->tick();
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->readMethodFrame($listener, $client);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);
            fclose($client);
            for ($i = 0; $i < 10; $i++) {
                $listener->tick();
                usleep(1000);
            }
        } finally {
            $listener->stop();
        }

        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testHeartbeatTimeoutReleasesUnackedDeliveryAndCleansConsumerRegistry(): void
    {
        $now = 0;
        $connections = new ConnectionRegistry();
        $consumers = new ConsumerRegistry();
        $listener = new AmqpListener(
            $connections,
            '127.0.0.1',
            0,
            broker: $this->broker(),
            authenticator: $this->authenticator(),
            authorizer: $this->authorizer(),
            consumers: $consumers,
            heartbeatInterval: 1,
            clock: static function () use (&$now): int {
                return $now * 1_000_000_000;
            }
        );
        $listener->start();
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, 4)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, 'held')));
            $listener->tick();
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->readMethodFrame($listener, $client);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);
            self::assertSame(1, $connections->count());
            self::assertSame(1, $consumers->count());

            $now = 2;
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(0, $connections->count());
        self::assertSame(0, $consumers->count());
        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testCleanupIsIdempotentAfterDisconnect(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, 4)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, 'held')));
            $listener->tick();
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->readMethodFrame($listener, $client);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);
            fclose($client);
            $listener->tick();
            $listener->stop();
            $listener->stop();
        } finally {
            $listener->stop();
        }

        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testExclusiveConsumerEndToEndRejectsConflictsAndReleasesOnCancel(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(3, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));

            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'exclusive-a', exclusive: true));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame(1, $consumers->count());
            self::assertTrue($consumers->hasExclusiveConsumer('/', 'orders'));

            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            self::assertSame('one', $firstBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($firstTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 2, 60, 20, $this->basicConsume('orders', 'normal-b'));
            $normalClose = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $normalClose->method());
            self::assertSame(2, $normalClose->channel);
            self::assertSame(403, $this->replyCode($normalClose));
            $this->sendMethod($client, 2, 20, 41);

            $this->sendMethod($client, 3, 60, 20, $this->basicConsume('orders', 'exclusive-b', exclusive: true));
            $exclusiveClose = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $exclusiveClose->method());
            self::assertSame(3, $exclusiveClose->channel);
            self::assertSame(403, $this->replyCode($exclusiveClose));
            $this->sendMethod($client, 3, 20, 41);
            self::assertSame(1, $listener->connectionCount());
            self::assertSame(1, $consumers->count());

            $this->publishBody($listener, $client, 1, '', 'orders', 'two');
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            self::assertSame('two', $secondBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($secondTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('exclusive-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertSame(0, $consumers->count());
            self::assertFalse($consumers->hasExclusiveConsumer('/', 'orders'));

            fwrite($client, $codec->encode(Frame::methodFrame(4, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->publishBody($listener, $client, 1, '', 'orders', 'three');
            $this->sendMethod($client, 4, 60, 20, $this->basicConsume('orders', 'normal-c'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $replacementDelivery = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $replacementDelivery->method());
            self::assertSame(4, $replacementDelivery->channel);
            self::assertSame('normal-c', $this->consumerTagFromDeliver($replacementDelivery));
            $replacementTag = $this->deliveryTagFromDeliver($replacementDelivery);
            $this->readFrame($listener, $client);
            self::assertSame('three', $this->readFrame($listener, $client)->payload);
            $this->sendMethod($client, 4, 60, 80, $this->packLongLong($replacementTag) . "\x00");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged],
            $this->deliveryStates('orders')
        );
    }

    public function testExclusiveConsumerIsRejectedWhenNormalConsumerAlreadyExistsAndOtherQueuesAreUnaffected(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(3, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('invoices'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'normal-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 2, 60, 20, $this->basicConsume('orders', 'exclusive-b', exclusive: true));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(2, $close->channel);
            self::assertSame(403, $this->replyCode($close));
            self::assertSame(1, $listener->connectionCount());

            $this->publishBody($listener, $client, 1, '', 'invoices', 'unrelated');
            $this->sendMethod($client, 3, 60, 20, $this->basicConsume('invoices', 'exclusive-other', exclusive: true));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $delivery = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $delivery->method());
            self::assertSame(3, $delivery->channel);
            $this->readFrame($listener, $client);
            self::assertSame('unrelated', $this->readFrame($listener, $client)->payload);
            self::assertSame(2, $consumers->count());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testChannelCloseReleasesExclusiveConsumer(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'exclusive-a', exclusive: true));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 20, 40, pack('n', 200) . $this->shortString('Goodbye') . pack('nn', 0, 0))));
            self::assertSame([20, 41], $this->readMethod($listener, $client));
            self::assertSame(0, $consumers->count());

            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 60, 20, $this->basicConsume('orders', 'normal-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testDisconnectReleasesExclusiveConsumer(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'exclusive-a', exclusive: true));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame(1, $consumers->count());

            fclose($client);
            $listener->tick();
            self::assertSame(0, $consumers->count());

            $replacement = $this->connectClient($listener, 1);
            $this->sendMethod($replacement, 1, 60, 20, $this->basicConsume('orders', 'normal-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $replacement));
            fclose($replacement);
        } finally {
            if (is_resource($client)) {
                fclose($client);
            }
            $listener->stop();
        }
    }

    public function testHeartbeatTimeoutReleasesExclusiveConsumer(): void
    {
        $now = 0;
        $consumers = new ConsumerRegistry();
        $listener = new AmqpListener(
            new ConnectionRegistry(),
            '127.0.0.1',
            0,
            broker: $this->broker(),
            authenticator: $this->authenticator(),
            authorizer: $this->authorizer(),
            consumers: $consumers,
            heartbeatInterval: 1,
            clock: static function () use (&$now): int {
                return $now * 1_000_000_000;
            }
        );
        $listener->start();
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);

        try {
            $this->connectAndOpenChannel($listener, $client, 1, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'exclusive-a', exclusive: true));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame(1, $consumers->count());

            $now = 2;
            $listener->tick();
            self::assertSame(0, $consumers->count());

            $replacement = $this->connectClient($listener, 1);
            $this->sendMethod($replacement, 1, 60, 20, $this->basicConsume('orders', 'normal-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $replacement));
            fclose($replacement);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testNoLocalConsumerRemainsSeparatelyUnsupported(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a', noLocal: true));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(540, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testBasicQosReturnsQosOk(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));

            self::assertSame([60, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testConsumerLimitPerChannelClosesRejectedChannelAndRuntimeStaysHealthy(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxConsumersPerChannel: 1));

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('invoices'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('invoices', 'consumer-b'));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(506, $this->replyCode($close));
            self::assertSame(1, $listener->connectionCount());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testConsumerLimitPerConnectionClosesOnlyRejectedChannel(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxConsumersPerConnection: 1));
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('invoices'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 60, 20, $this->basicConsume('invoices', 'consumer-b'));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(506, $this->replyCode($close));

            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, 4)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, 'ping')));
            $body = $this->readDeliveryBody($listener, $client);
            self::assertSame('ping', $body);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testMessageAtConfiguredSizeLimitPublishesAndConsumes(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxMessageSize: 5));

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['12345']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame('12345', $this->readDeliveryBody($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testOversizedDeclaredMessageIsRejectedBeforePersistence(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxMessageSize: 5));
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, 6)));
            $this->awaitEof($listener, $client);
            self::assertSame(0, $this->tableCount('messages'));
        } finally {
            if (is_resource($client)) {
                fclose($client);
            }
            $listener->stop();
        }
    }

    public function testQueueDepthLimitOnPublishClosesChannelWithResourceError(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxQueueDepth: 1));

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, '', 'orders', 'first');
            $this->publishBody($listener, $client, 1, '', 'orders', 'second');
            $close = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $close->method());
            self::assertSame(506, $this->replyCode($close));
            self::assertSame(1, $this->tableCount('messages'));
            self::assertSame(1, $listener->connectionCount());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testZeroQueueDepthLimitAllowsUnlimitedAmqpPublishes(): void
    {
        [$listener, $client] = $this->startedListener(limits: new ResourceLimits(maxQueueDepth: 0));

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three']);

            self::assertSame(3, $this->tableCount('messages'));
            self::assertSame(3, $this->tableCount('deliveries'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPrefetchOneDeliversOnlyOneUnackedMessage(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first', 'second']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->readDeliveryBody($listener, $client);
            $this->assertNoFrame($listener, $client);
            self::assertSame([DeliveryState::Reserved, DeliveryState::Pending], $this->deliveryStates('orders'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testBasicCancelAcknowledgesConsumerAndChannelRemainsUsable(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            for ($i = 0; $i < 3; $i++) {
                $delivery = $this->readMethodFrame($listener, $client);
                self::assertSame([60, 60], $delivery->method());
                $deliveryTag = $this->deliveryTagFromDeliver($delivery);
                $this->readFrame($listener, $client);
                $this->readFrame($listener, $client);
                $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            }

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            $cancelOk = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 31], $cancelOk->method());
            self::assertSame('consumer-a', $this->consumerTagFromBasicCancelOk($cancelOk));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 20, 40, pack('n', 200) . $this->shortString('Goodbye') . pack('nn', 0, 0));
            self::assertSame([20, 41], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged],
            $this->deliveryStates('orders')
        );
    }

    public function testAutoDeleteQueueLifecycleDeliversThenDeletesAfterFinalCancel(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.orders', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            self::assertTrue($this->queueExists('auto.orders'));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.orders', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertTrue($this->queueExists('auto.orders'));

            $this->publishBody($listener, $client, 1, '', 'auto.orders', 'hello-auto-delete');
            $deliver = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $deliver->method());
            $deliveryTag = $this->deliveryTagFromDeliver($deliver);
            $this->readFrame($listener, $client);
            self::assertSame('hello-auto-delete', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertFalse($this->queueExists('auto.orders'));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.orders', passive: true));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(404, $this->replyCode($close));

            $this->sendMethod($client, 2, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 50, 10, $this->queueDeclare('auto.orders', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            self::assertTrue($this->queueExists('auto.orders'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testAutoDeleteQueueWithMultipleConsumersDeletesOnlyAfterFinalCancel(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.multi', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.multi', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.multi', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertTrue($this->queueExists('auto.multi'));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.multi', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-b'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertFalse($this->queueExists('auto.multi'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testAutoDeleteQueueCleanupIgnoresConsumersOnAnotherQueue(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.shared-a', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.shared-b', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.shared-a', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.shared-b', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));

            self::assertFalse($this->queueExists('auto.shared-a'));
            self::assertTrue($this->queueExists('auto.shared-b'));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.shared-b', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testAutoDeleteQueueDeletesWhenConsumerChannelCloses(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.channel', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.channel', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(1, 20, 40, pack('n', 200) . $this->shortString('Goodbye') . pack('nn', 0, 0))));
            self::assertSame([20, 41], $this->readMethod($listener, $client));

            self::assertFalse($this->queueExists('auto.channel'));
            $this->sendMethod($client, 2, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 50, 10, $this->queueDeclare('auto.channel', passive: true));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(404, $this->replyCode($close));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testAutoDeleteQueueDeletesWhenConsumerConnectionClosesGracefully(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.connection-close', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.connection-close', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            fwrite($client, $codec->encode(Frame::methodFrame(0, 10, 50, pack('n', 200) . $this->shortString('Goodbye') . pack('nn', 0, 0))));
            self::assertSame([10, 51], $this->readMethod($listener, $client));
            $this->awaitEof($listener, $client);
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertFalse($this->queueExists('auto.connection-close'));
    }

    public function testAutoDeleteQueueDeletesWhenConsumerConnectionDisconnects(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.disconnect', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.disconnect', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            fclose($client);
            for ($attempt = 0; $attempt < 10; $attempt++) {
                $listener->tick();
                usleep(1000);
            }
        } finally {
            $listener->stop();
        }

        self::assertFalse($this->queueExists('auto.disconnect'));
    }

    public function testAutoDeleteQueueDeletesWhenHeartbeatTimeoutRemovesConsumer(): void
    {
        $now = 0;
        $listener = new AmqpListener(
            new ConnectionRegistry(),
            '127.0.0.1',
            0,
            broker: $this->broker(),
            authenticator: $this->authenticator(),
            authorizer: $this->authorizer(),
            consumers: new ConsumerRegistry(),
            heartbeatInterval: 1,
            clock: static function () use (&$now): int {
                return $now * 1_000_000_000;
            }
        );
        $listener->start();
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);

        try {
            $this->connectAndOpenChannel($listener, $client, 1, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.heartbeat', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('auto.heartbeat', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $now = 2;
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertFalse($this->queueExists('auto.heartbeat'));
    }

    public function testAutoDeleteQueueWithoutConsumerSurvivesZeroConsumerCleanupPaths(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.never-used', durable: false, autoDelete: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('auto.never-used', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));

            fclose($client);
            for ($attempt = 0; $attempt < 10; $attempt++) {
                $listener->tick();
                usleep(1000);
            }
        } finally {
            $listener->stop();
        }

        self::assertTrue($this->queueExists('auto.never-used'));
    }

    public function testNonAutoDeleteQueueSurvivesFinalConsumerCancel(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('manual.orders', durable: false, autoDelete: false));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('manual.orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('manual.orders', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertTrue($this->queueExists('manual.orders'));
    }

    public function testBasicCancelBeforeAnyDeliveryRemovesConsumerAndSendsNoFurtherDeliveries(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame(1, $consumers->count());

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertSame(0, $consumers->count());

            $this->publishBody($listener, $client, 1, '', 'orders', 'after-cancel');
            $this->assertNoFrame($listener, $client);
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testBasicCancelReleasesOutstandingUnackedDelivery(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['held']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame([60, 60], $this->readMethod($listener, $client));
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);
            self::assertSame(DeliveryState::Reserved, $this->singleDelivery('orders')->state);

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testBasicCancelDoesNotRequeueAlreadyAcknowledgedDelivery(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['done']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $delivery = $this->readMethodFrame($listener, $client);
            $deliveryTag = $this->deliveryTagFromDeliver($delivery);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Acknowledged, $this->singleDelivery('orders')->state);
    }

    public function testAckThenCancelRemovesDeliveryFromUnsettledTracking(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['done']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$deliveryTag, $body] = $this->readDelivery($listener, $client);
            self::assertSame('done', $body);
            self::assertSame(1, $listener->inFlightCount());

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($deliveryTag) . "\x00");
            $listener->tick();
            self::assertSame(0, $listener->inFlightCount());

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertSame(0, $listener->inFlightCount());

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Acknowledged, $this->singleDelivery('orders')->state);
    }

    public function testBasicCancelReleasesOnlyMixedUnacknowledgedDeliveries(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first', 'second']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            self::assertSame('first', $firstBody);
            self::assertSame('second', $secondBody);
            self::assertSame(2, $listener->inFlightCount());

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($firstTag) . "\x00");
            $listener->tick();
            self::assertSame(1, $listener->inFlightCount());

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertSame(1, $listener->inFlightCount());
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Pending],
            $this->deliveryStates('orders')
        );
    }

    public function testRejectAndNackSettlementRemoveDeliveryFromCancellationCleanup(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['reject', 'nack']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$rejectTag, $rejectBody] = $this->readDelivery($listener, $client);
            [$nackTag, $nackBody] = $this->readDelivery($listener, $client);
            self::assertSame('reject', $rejectBody);
            self::assertSame('nack', $nackBody);

            $this->sendMethod($client, 1, 60, 90, $this->packLongLong($rejectTag) . "\x00");
            $listener->tick();
            self::assertSame(1, $listener->inFlightCount());
            $this->sendMethod($client, 1, 60, 120, $this->basicNack($nackTag, false, false));
            $listener->tick();
            self::assertSame(0, $listener->inFlightCount());

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Rejected, DeliveryState::Rejected],
            $this->deliveryStates('orders')
        );
    }

    public function testRepeatedCancellationAndReplacementConsumerKeepChannelUsable(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['one', 'two', 'three']);

            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            self::assertSame('one', $firstBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($firstTag) . "\x00");
            [, $secondBody] = $this->readDelivery($listener, $client);
            self::assertSame('two', $secondBody);

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertSame(1, $listener->inFlightCount());

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(404, $this->replyCode($close));
            $this->sendMethod($client, 1, 20, 41, pack('n', 200) . $this->shortString('OK') . pack('nn', 0, 0));
            $this->sendMethod($client, 1, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$redeliveredSecondTag, $redeliveredSecondBody] = $this->readDelivery($listener, $client);
            self::assertSame('two', $redeliveredSecondBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($redeliveredSecondTag) . "\x00");
            [$thirdTag, $thirdBody] = $this->readDelivery($listener, $client);
            self::assertSame('three', $thirdBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($thirdTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-b'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged],
            $this->deliveryStates('orders')
        );
    }

    public function testAckCancelReplacementConsumerAndReusedChannelEndToEnd(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['Message 1', 'Message 2', 'Message 3']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 1', $firstBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($firstTag) . "\x00");
            [$secondHeldTag, $secondHeldBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 2', $secondHeldBody);
            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 90, $this->packLongLong($secondHeldTag) . "\x01");
            $listener->tick();

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $depthAfterConsumerA = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 11], $depthAfterConsumerA->method());
            self::assertSame(2, $this->queueDeclareMessageCount($depthAfterConsumerA));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 2', $secondBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($secondTag) . "\x00");
            [$thirdTag, $thirdBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 3', $thirdBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($thirdTag) . "\x00");
            $listener->tick();
            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-b'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $emptyDepth = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 11], $emptyDepth->method());
            self::assertSame(0, $this->queueDeclareMessageCount($emptyDepth));

            $this->publishBody($listener, $client, 1, '', 'orders', 'Message 4');
            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders'));
            $getOk = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 71], $getOk->method());
            $getTag = $this->deliveryTagFromBasicGetOk($getOk);
            $this->readFrame($listener, $client);
            self::assertSame('Message 4', $this->readFrame($listener, $client)->payload);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($getTag) . "\x00");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
            ],
            $this->deliveryStates('orders')
        );
    }

    public function testImmediateCancelAfterAckReleasesAlreadySentUnackedDeliveries(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['Message 1', 'Message 2', 'Message 3']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 1', $firstBody);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($firstTag) . "\x00");
            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            $framesBeforeCancelOk = $this->readUntilCancelOk($listener, $client);
            self::assertSame([], $framesBeforeCancelOk);
            self::assertSame(0, $listener->inFlightCount());

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $depthAfterConsumerA = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 11], $depthAfterConsumerA->method());
            self::assertSame(2, $this->queueDeclareMessageCount($depthAfterConsumerA));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 2', $secondBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($secondTag) . "\x00");
            [$thirdTag, $thirdBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 3', $thirdBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($thirdTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-b'));
            self::assertSame([], $this->readUntilCancelOk($listener, $client));

            $this->publishBody($listener, $client, 1, '', 'orders', 'Message 4');
            $this->sendMethod($client, 1, 60, 70, $this->basicGet('orders'));
            $getOk = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 71], $getOk->method());
            $getTag = $this->deliveryTagFromBasicGetOk($getOk);
            $this->readFrame($listener, $client);
            self::assertSame('Message 4', $this->readFrame($listener, $client)->payload);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($getTag) . "\x00");
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
                DeliveryState::Acknowledged,
            ],
            $this->deliveryStates('orders')
        );
    }

    public function testCancelAfterAckUsesFluxQDeliveryIdWhenAmqpTagDiffers(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'seed', ['seed']);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['Message 1', 'Message 2', 'Message 3']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 0, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            [$thirdTag, $thirdBody] = $this->readDelivery($listener, $client);
            self::assertSame(1, $firstTag);
            self::assertSame([1, 2, 3], [$firstTag, $secondTag, $thirdTag]);
            self::assertSame(['Message 1', 'Message 2', 'Message 3'], [$firstBody, $secondBody, $thirdBody]);

            $ordersBeforeAck = $this->deliveriesForQueue('orders');
            self::assertSame('1', $ordersBeforeAck[0]->deliveryTag);
            self::assertNotSame($firstTag, $ordersBeforeAck[0]->id);
            self::assertSame(DeliveryState::Reserved, $ordersBeforeAck[0]->state);

            fwrite(
                $client,
                $codec->encode(Frame::methodFrame(1, 60, 80, $this->packLongLong($firstTag) . "\x00"))
                . $codec->encode(Frame::methodFrame(1, 60, 90, $this->packLongLong($secondTag) . "\x01"))
                . $codec->encode(Frame::methodFrame(1, 60, 90, $this->packLongLong($thirdTag) . "\x01"))
                . $codec->encode(Frame::methodFrame(1, 60, 30, $this->basicCancel('consumer-a')))
            );
            self::assertSame([], $this->readUntilCancelOk($listener, $client));

            $ordersAfterCancel = $this->deliveriesForQueue('orders');
            self::assertSame(DeliveryState::Acknowledged, $ordersAfterCancel[0]->state);
            self::assertSame('1', $ordersAfterCancel[0]->deliveryTag);
            self::assertSame(DeliveryState::Pending, $ordersAfterCancel[1]->state);
            self::assertNull($ordersAfterCancel[1]->deliveryTag);
            self::assertSame(DeliveryState::Pending, $ordersAfterCancel[2]->state);
            self::assertNull($ordersAfterCancel[2]->deliveryTag);

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 2', $secondBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($secondTag) . "\x00");
            [$thirdTag, $thirdBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 3', $thirdBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($thirdTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-b'));
            self::assertSame([], $this->readUntilCancelOk($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $emptyDepth = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 11], $emptyDepth->method());
            self::assertSame(0, $this->queueDeclareMessageCount($emptyDepth));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged],
            $this->deliveryStates('orders')
        );
    }

    public function testPikaStopConsumingRejectsPendingDeliveriesBeforeCancelWithoutRedeliveryRace(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['Message 1', 'Message 2', 'Message 3']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 0, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$firstTag, $firstBody] = $this->readDelivery($listener, $client);
            [$secondTag, $secondBody] = $this->readDelivery($listener, $client);
            [$thirdTag, $thirdBody] = $this->readDelivery($listener, $client);
            self::assertSame([1, 2, 3], [$firstTag, $secondTag, $thirdTag]);
            self::assertSame(['Message 1', 'Message 2', 'Message 3'], [$firstBody, $secondBody, $thirdBody]);

            fwrite(
                $client,
                $codec->encode(Frame::methodFrame(1, 60, 80, $this->packLongLong($firstTag) . "\x00"))
                . $codec->encode(Frame::methodFrame(1, 60, 90, $this->packLongLong($secondTag) . "\x01"))
                . $codec->encode(Frame::methodFrame(1, 60, 90, $this->packLongLong($thirdTag) . "\x01"))
                . $codec->encode(Frame::methodFrame(1, 60, 30, $this->basicCancel('consumer-a')))
            );

            self::assertSame([], $this->readUntilCancelOk($listener, $client));
            self::assertSame(0, $listener->inFlightCount());

            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders', passive: true));
            $depthAfterConsumerA = $this->readMethodFrame($listener, $client);
            self::assertSame([50, 11], $depthAfterConsumerA->method());
            self::assertSame(2, $this->queueDeclareMessageCount($depthAfterConsumerA));

            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            [$redeliveredSecondTag, $redeliveredSecondBody] = $this->readDelivery($listener, $client);
            [$redeliveredThirdTag, $redeliveredThirdBody] = $this->readDelivery($listener, $client);
            self::assertSame('Message 2', $redeliveredSecondBody);
            self::assertSame('Message 3', $redeliveredThirdBody);
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($redeliveredSecondTag) . "\x00");
            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($redeliveredThirdTag) . "\x00");
            $listener->tick();

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-b'));
            self::assertSame([], $this->readUntilCancelOk($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(
            [DeliveryState::Acknowledged, DeliveryState::Acknowledged, DeliveryState::Acknowledged],
            $this->deliveryStates('orders')
        );
    }

    public function testBasicCancelLeavesAnotherConsumerOnSameChannelActive(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders-a'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders-b'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders-a', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders-b', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            self::assertSame(1, $consumers->count());

            $this->publishBody($listener, $client, 1, '', 'orders-b', 'still-active');
            $delivery = $this->readMethodFrame($listener, $client);
            self::assertSame([60, 60], $delivery->method());
            self::assertSame('consumer-b', $this->consumerTagFromDeliver($delivery));
            $this->readFrame($listener, $client);
            self::assertSame('still-active', $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testBasicCancelNoWaitSendsNoCancelOkButCancelsConsumer(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a', noWait: true));
            $this->assertNoFrame($listener, $client);
            self::assertSame(0, $consumers->count());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testRepeatedCleanupAfterBasicCancelAndConnectionCloseIsSafe(): void
    {
        $consumers = new ConsumerRegistry();
        [$listener, $client] = $this->startedListener(consumers: $consumers);

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['held']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame([60, 60], $this->readMethod($listener, $client));
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);

            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('consumer-a'));
            self::assertSame([60, 31], $this->readMethod($listener, $client));
            fclose($client);
            for ($attempt = 0; $attempt < 10; $attempt++) {
                $listener->tick();
                usleep(1000);
            }
        } finally {
            $listener->stop();
        }

        self::assertSame(0, $consumers->count());
        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testUnknownBasicCancelTagClosesOnlyChannelAndRuntimeRemainsHealthy(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 30, $this->basicCancel('missing'));
            $close = $this->readMethodFrame($listener, $client);
            self::assertSame([20, 40], $close->method());
            self::assertSame(404, $this->replyCode($close));

            $this->sendMethod($client, 2, 20, 10, "\x00");
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testMalformedBasicCancelDoesNotStopListener(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 30, '');
            for ($attempt = 0; $attempt < 10; $attempt++) {
                $listener->tick();
                usleep(1000);
            }
            fclose($client);

            $nextClient = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
            self::assertIsResource($nextClient, sprintf('Could not reconnect to AMQP listener: %s', $errorMessage));
            stream_set_blocking($nextClient, false);

            try {
                $this->connectAndOpenChannel($listener, $nextClient, 1);
                $this->sendMethod($nextClient, 1, 50, 10, $this->queueDeclare('orders'));
                self::assertSame([50, 11], $this->readMethod($listener, $nextClient));
            } finally {
                fclose($nextClient);
            }
        } finally {
            $listener->stop();
        }
    }

    public function testAckFreesPrefetchCapacityAndAllowsNextDelivery(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first', 'second']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $first = $this->readMethodFrame($listener, $client);
            $firstTag = $this->deliveryTagFromDeliver($first);
            $this->readFrame($listener, $client);
            self::assertSame('first', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 80, $this->packLongLong($firstTag) . "\x00");
            $second = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 60], $second->method());
            $this->readFrame($listener, $client);
            self::assertSame('second', $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testRejectFreesPrefetchCapacity(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first', 'second']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $first = $this->readMethodFrame($listener, $client);
            $firstTag = $this->deliveryTagFromDeliver($first);
            $this->readFrame($listener, $client);
            self::assertSame('first', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 90, $this->packLongLong($firstTag) . "\x00");
            $second = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 60], $second->method());
            $this->readFrame($listener, $client);
            self::assertSame('second', $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Rejected, DeliveryState::Pending], $this->deliveryStates('orders'));
    }

    public function testNackRequeueFalseRejectsDelivery(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $deliver = $this->readMethodFrame($listener, $client);
            $deliveryTag = $this->deliveryTagFromDeliver($deliver);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);

            $this->sendMethod($client, 1, 60, 120, $this->basicNack($deliveryTag, false, false));
            $listener->tick();
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Rejected, $this->singleDelivery('orders')->state);
    }

    public function testNackRequeueTrueReleasesAndRedelivers(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $first = $this->readMethodFrame($listener, $client);
            $firstTag = $this->deliveryTagFromDeliver($first);
            $this->readFrame($listener, $client);
            self::assertSame('first', $this->readFrame($listener, $client)->payload);

            $this->sendMethod($client, 1, 60, 120, $this->basicNack($firstTag, false, true));
            $second = $this->readMethodFrame($listener, $client);
            $secondTag = $this->deliveryTagFromDeliver($second);

            self::assertSame([60, 60], $second->method());
            self::assertNotSame($firstTag, $secondTag);
            $this->readFrame($listener, $client);
            self::assertSame('first', $this->readFrame($listener, $client)->payload);
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Pending, $this->singleDelivery('orders')->state);
    }

    public function testPrefetchZeroIsUnlimited(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first', 'second']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 0, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));

            self::assertSame('first', $this->readDeliveryBody($listener, $client));
            self::assertSame('second', $this->readDeliveryBody($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Pending, DeliveryState::Pending], $this->deliveryStates('orders'));
    }

    public function testUnsupportedPrefetchSizeIsRejected(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(1, 1, false));

            self::assertSame([20, 40], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testUnsupportedQosGlobalModeIsRejected(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, true));

            self::assertSame([20, 40], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testInvalidNackDeliveryTagFailsSafelyAndRuntimeContinues(): void
    {
        $connections = new ConnectionRegistry();
        $listener = new AmqpListener(
            $connections,
            '127.0.0.1',
            0,
            broker: $this->broker(),
            authenticator: $this->authenticator(),
            authorizer: $this->authorizer(),
            consumers: new ConsumerRegistry()
        );
        $listener->start();
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 60, 120, $this->basicNack(99, false, false));

            self::assertSame([20, 40], $this->readMethod($listener, $client));
            self::assertSame(1, $connections->count());
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(0, $connections->count());
    }

    public function testNackMultipleTrueSettlesMatchingOutstandingDelivery(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first']);
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            $deliver = $this->readMethodFrame($listener, $client);
            $deliveryTag = $this->deliveryTagFromDeliver($deliver);
            $this->readFrame($listener, $client);
            $this->readFrame($listener, $client);

            $this->sendMethod($client, 1, 60, 120, $this->basicNack($deliveryTag, true, false));
            $listener->tick();
            self::assertSame(0, $listener->inFlightCount());
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame(DeliveryState::Rejected, $this->singleDelivery('orders')->state);
    }

    public function testMultipleChannelsKeepPrefetchLimitsSeparate(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->declareQueueAndPublishBodies($listener, $client, 'orders', ['first', 'second']);
            $this->sendMethod($client, 1, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 60, 10, $this->basicQos(0, 1, false));
            self::assertSame([60, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 20, $this->basicConsume('orders', 'consumer-a'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame('first', $this->readDeliveryBody($listener, $client));
            $this->sendMethod($client, 2, 60, 20, $this->basicConsume('orders', 'consumer-b'));
            self::assertSame([60, 21], $this->readMethod($listener, $client));
            self::assertSame('second', $this->readDeliveryBody($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }

        self::assertSame([DeliveryState::Pending, DeliveryState::Pending], $this->deliveryStates('orders'));
    }

    public function testConfirmSelectReturnsSelectOk(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));

            self::assertSame([85, 11], $this->readMethod($listener, $client));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testConfirmSelectNoWaitDoesNotSendSelectOkButPublishesAreConfirmed(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(true));
            $this->publishBody($listener, $client, 1, '', 'orders', 'confirmed');
            $ack = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertFalse($this->multipleFromBasicAck($ack));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testPublishAfterConfirmModeReceivesIndividualBasicAck(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, '', 'orders', 'confirmed');
            $ack = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertFalse($this->multipleFromBasicAck($ack));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testSubsequentConfirmPublishesIncrementDeliveryTags(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, '', 'orders', 'first');
            self::assertSame(1, $this->deliveryTagFromBasicAck($this->readMethodFrame($listener, $client)));
            $this->publishBody($listener, $client, 1, '', 'orders', 'second');
            self::assertSame(2, $this->deliveryTagFromBasicAck($this->readMethodFrame($listener, $client)));
            $this->publishBody($listener, $client, 1, '', 'orders', 'third');
            self::assertSame(3, $this->deliveryTagFromBasicAck($this->readMethodFrame($listener, $client)));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testSeparateChannelsMaintainIndependentConfirmCounters(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            fwrite($client, $codec->encode(Frame::methodFrame(2, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 2, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));

            $this->publishBody($listener, $client, 1, '', 'orders', 'first');
            $ackOne = $this->readMethodFrame($listener, $client);
            $this->publishBody($listener, $client, 2, '', 'orders', 'second');
            $ackTwo = $this->readMethodFrame($listener, $client);

            self::assertSame(1, $ackOne->channel);
            self::assertSame(1, $this->deliveryTagFromBasicAck($ackOne));
            self::assertSame(2, $ackTwo->channel);
            self::assertSame(1, $this->deliveryTagFromBasicAck($ackTwo));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testNonConfirmChannelReceivesNoPublisherConfirm(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->publishBody($listener, $client, 1, '', 'orders', 'unconfirmed');

            $this->assertNoFrame($listener, $client);
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testSplitBinaryPublishIsConfirmedOnlyAfterCompletePersistence(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();
        $payload = "first\x00second\xff";

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', 'orders'));
            fwrite($client, $codec->encode($this->contentHeader(1, strlen($payload), 'application/octet-stream', 5)));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, substr($payload, 0, 5))));
            $listener->tick();
            $this->assertNoFrame($listener, $client);
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, substr($payload, 5))));
            $ack = $this->readMethodFrame($listener, $client);

            self::assertSame([60, 80], $ack->method());
            self::assertSame(1, $this->deliveryTagFromBasicAck($ack));
            self::assertSame($payload, $this->payloadForSingleMessage('orders'));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testFailedConfirmPublishIsNotPositivelyAcknowledged(): void
    {
        [$listener, $client] = $this->startedListener();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));
            $this->publishBody($listener, $client, 1, 'missing.exchange', 'orders', 'failed');
            $frame = $this->readMethodFrame($listener, $client);

            self::assertSame([20, 40], $frame->method());
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    public function testChannelCloseClearsConfirmStateAndSequenceTracking(): void
    {
        [$listener, $client] = $this->startedListener();
        $codec = new FrameCodec();

        try {
            $this->connectAndOpenChannel($listener, $client, 1);
            $this->sendMethod($client, 1, 50, 10, $this->queueDeclare('orders'));
            self::assertSame([50, 11], $this->readMethod($listener, $client));
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));
            $this->publishBody($listener, $client, 1, '', 'orders', 'first');
            self::assertSame(1, $this->deliveryTagFromBasicAck($this->readMethodFrame($listener, $client)));
            fwrite($client, $codec->encode(Frame::methodFrame(1, 20, 40, pack('n', 200) . $this->shortString('Goodbye') . pack('nn', 0, 0))));
            self::assertSame([20, 41], $this->readMethod($listener, $client));
            fwrite($client, $codec->encode(Frame::methodFrame(1, 20, 10, "\x00")));
            self::assertSame([20, 11], $this->readMethod($listener, $client));
            $this->publishBody($listener, $client, 1, '', 'orders', 'second');
            $this->assertNoFrame($listener, $client);
            $this->sendMethod($client, 1, 85, 10, $this->confirmSelect(false));
            self::assertSame([85, 11], $this->readMethod($listener, $client));
            $this->publishBody($listener, $client, 1, '', 'orders', 'third');
            self::assertSame(1, $this->deliveryTagFromBasicAck($this->readMethodFrame($listener, $client)));
        } finally {
            fclose($client);
            $listener->stop();
        }
    }

    /**
     * @return array{0: AmqpListener, 1: resource}
     */
    private function startedListener(
        ?ConnectionRegistry $connections = null,
        ?ResourceLimits $limits = null,
        ?ConsumerRegistry $consumers = null
    ): array
    {
        $listener = new AmqpListener(
            $connections ?? new ConnectionRegistry(),
            '127.0.0.1',
            0,
            maxFrameSize: $limits?->maxFrameSize ?? 131072,
            broker: $this->broker($limits),
            authenticator: $this->authenticator(),
            authorizer: $this->authorizer(),
            consumers: $consumers ?? new ConsumerRegistry(),
            maxMessageSize: $limits?->maxMessageSize ?? 10485760,
            limits: $limits
        );
        $listener->start();
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);

        return [$listener, $client];
    }

    /**
     * @return array{0: AmqpListener, 1: resource}
     */
    private function startedTlsListener(?ConnectionRegistry $connections = null, ?ResourceLimits $limits = null): array
    {
        $listener = new AmqpListener(
            $connections ?? new ConnectionRegistry(),
            '127.0.0.1',
            0,
            maxFrameSize: $limits?->maxFrameSize ?? 131072,
            broker: $this->broker($limits),
            authenticator: $this->authenticator(),
            authorizer: $this->authorizer(),
            consumers: new ConsumerRegistry(),
            maxMessageSize: $limits?->maxMessageSize ?? 10485760,
            tls: $this->tlsConfig(),
            limits: $limits
        );
        $listener->start();

        return [$listener, $this->connectTls($listener)];
    }

    /**
     * @param resource $client
     */
    private function connectTls(AmqpListener $listener): mixed
    {
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP TLS listener: %s', $errorMessage));
        stream_set_blocking($client, false);
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
     */
    private function connectAndOpenChannel(AmqpListener $listener, mixed $client, int $channel, int $heartbeat = 60): void
    {
        $codec = new FrameCodec();
        fwrite($client, AmqpConnection::PROTOCOL_HEADER);
        self::assertSame([10, 10], $this->readMethod($listener, $client));
        $this->sendMethod($client, 0, 10, 11, $this->startOk());
        self::assertSame([10, 30], $this->readMethod($listener, $client));
        $this->sendMethod($client, 0, 10, 31, $this->tuneOk($heartbeat));
        $listener->tick();
        $this->sendMethod($client, 0, 10, 40, $this->connectionOpen('/'));
        self::assertSame([10, 41], $this->readMethod($listener, $client));
        fwrite($client, $codec->encode(Frame::methodFrame($channel, 20, 10, "\x00")));
        self::assertSame([20, 11], $this->readMethod($listener, $client));
    }

    /**
     * @return resource
     */
    private function connectClient(AmqpListener $listener, int $channel, int $heartbeat = 60): mixed
    {
        $client = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $listener->port()), $errorCode, $errorMessage, 1.0);
        self::assertIsResource($client, sprintf('Could not connect to AMQP listener: %s', $errorMessage));
        stream_set_blocking($client, false);
        $this->connectAndOpenChannel($listener, $client, $channel, $heartbeat);

        return $client;
    }

    /**
     * @param resource $client
     */
    private function sendMethod(mixed $client, int $channel, int $classId, int $methodId, string $arguments = ''): void
    {
        fwrite($client, (new FrameCodec())->encode(Frame::methodFrame($channel, $classId, $methodId, $arguments)));
    }

    private function queueDeclare(
        string $queue,
        bool $passive = false,
        bool $durable = true,
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

    private function exchangeDeclare(string $exchange, string $type = 'direct'): string
    {
        return pack('n', 0) . $this->shortString($exchange) . $this->shortString($type) . "\x02" . pack('N', 0);
    }

    private function queueBind(string $queue, string $exchange, string $routingKey): string
    {
        return pack('n', 0) . $this->shortString($queue) . $this->shortString($exchange) . $this->shortString($routingKey) . "\x00" . pack('N', 0);
    }

    private function basicPublish(string $exchange, string $routingKey, bool $mandatory = false): string
    {
        return pack('n', 0) . $this->shortString($exchange) . $this->shortString($routingKey) . chr($mandatory ? 1 : 0);
    }

    private function basicConsume(
        string $queue,
        string $consumerTag,
        bool $noLocal = false,
        bool $noAck = false,
        bool $exclusive = false,
        bool $noWait = false
    ): string
    {
        $bits = 0;
        if ($noLocal) {
            $bits |= 0b00000001;
        }
        if ($noAck) {
            $bits |= 0b00000010;
        }
        if ($exclusive) {
            $bits |= 0b00000100;
        }
        if ($noWait) {
            $bits |= 0b00001000;
        }

        return pack('n', 0) . $this->shortString($queue) . $this->shortString($consumerTag) . chr($bits) . pack('N', 0);
    }

    private function basicCancel(string $consumerTag, bool $noWait = false): string
    {
        return $this->shortString($consumerTag) . chr($noWait ? 1 : 0);
    }

    private function basicGet(string $queue, bool $noAck = false): string
    {
        return pack('n', 0) . $this->shortString($queue) . chr($noAck ? 1 : 0);
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

    private function basicQos(int $prefetchSize, int $prefetchCount, bool $global): string
    {
        return pack('Nn', $prefetchSize, $prefetchCount) . chr($global ? 1 : 0);
    }

    private function basicNack(int $deliveryTag, bool $multiple, bool $requeue): string
    {
        $bits = 0;
        if ($multiple) {
            $bits |= 0b00000001;
        }
        if ($requeue) {
            $bits |= 0b00000010;
        }

        return $this->packLongLong($deliveryTag) . chr($bits);
    }

    private function confirmSelect(bool $noWait): string
    {
        return chr($noWait ? 1 : 0);
    }

    /**
     * @param resource $client
     */
    private function publishBody(
        AmqpListener $listener,
        mixed $client,
        int $channel,
        string $exchange,
        string $routingKey,
        string $body,
        bool $mandatory = false
    ): void {
        $codec = new FrameCodec();
        $this->sendMethod($client, $channel, 60, 40, $this->basicPublish($exchange, $routingKey, $mandatory));
        fwrite($client, $codec->encode($this->contentHeader($channel, strlen($body))));
        fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, $channel, $body)));
        $listener->tick();
    }

    /**
     * @param resource $client
     * @param array<string, mixed> $properties
     */
    private function publishBodyWithProperties(
        AmqpListener $listener,
        mixed $client,
        int $channel,
        string $exchange,
        string $routingKey,
        string $body,
        array $properties
    ): void {
        $codec = new FrameCodec();
        $this->sendMethod($client, $channel, 60, 40, $this->basicPublish($exchange, $routingKey));
        fwrite($client, $codec->encode($this->contentHeaderWithProperties($channel, strlen($body), $properties)));
        fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, $channel, $body)));
        $listener->tick();
    }

    /**
     * @param resource $client
     * @param list<string> $bodies
     */
    private function declareQueueAndPublishBodies(AmqpListener $listener, mixed $client, string $queue, array $bodies): void
    {
        $codec = new FrameCodec();
        $this->sendMethod($client, 1, 50, 10, $this->queueDeclare($queue));
        self::assertSame([50, 11], $this->readMethod($listener, $client));

        foreach ($bodies as $body) {
            $this->sendMethod($client, 1, 60, 40, $this->basicPublish('', $queue));
            fwrite($client, $codec->encode($this->contentHeader(1, strlen($body))));
            fwrite($client, $codec->encode(new Frame(Frame::TYPE_BODY, 1, $body)));
            $listener->tick();
        }
    }

    /**
     * @param resource $client
     */
    private function readDeliveryBody(AmqpListener $listener, mixed $client): string
    {
        $deliver = $this->readMethodFrame($listener, $client);
        self::assertSame([60, 60], $deliver->method());
        $this->readFrame($listener, $client);

        return $this->readFrame($listener, $client)->payload;
    }

    /**
     * @param resource $client
     * @return array{0: int, 1: string}
     */
    private function readDelivery(AmqpListener $listener, mixed $client): array
    {
        $deliver = $this->readMethodFrame($listener, $client);
        self::assertSame([60, 60], $deliver->method());
        $deliveryTag = $this->deliveryTagFromDeliver($deliver);
        $this->readFrame($listener, $client);

        return [$deliveryTag, $this->readFrame($listener, $client)->payload];
    }

    /**
     * @param resource $client
     * @return array{0: int, 1: bool, 2: string}
     */
    private function readBasicGetDelivery(
        AmqpListener $listener,
        mixed $client,
        string $queue,
        bool $noAck = false,
        int $channel = 1
    ): array {
        $this->sendMethod($client, $channel, 60, 70, $this->basicGet($queue, $noAck));
        $getOk = $this->readMethodFrame($listener, $client);
        self::assertSame([60, 71], $getOk->method());
        $deliveryTag = $this->deliveryTagFromBasicGetOk($getOk);
        $redelivered = $this->redeliveredFromBasicGetOk($getOk);
        $this->readFrame($listener, $client);

        return [$deliveryTag, $redelivered, $this->readFrame($listener, $client)->payload];
    }

    /**
     * @param resource $client
     */
    private function assertReadyMessageCount(AmqpListener $listener, mixed $client, string $queue, int $expected): void
    {
        $this->sendMethod($client, 1, 50, 10, $this->queueDeclare($queue, passive: true));
        $declareOk = $this->readMethodFrame($listener, $client);
        self::assertSame([50, 11], $declareOk->method());
        self::assertSame($expected, $this->queueDeclareMessageCount($declareOk));
    }

    /**
     * @param resource $client
     * @return list<string>
     */
    private function readUntilCancelOk(AmqpListener $listener, mixed $client): array
    {
        $deliveryBodies = [];

        for ($attempt = 0; $attempt < 10; $attempt++) {
            $frame = $this->readMethodFrame($listener, $client);
            if ($frame->method() === [60, 31]) {
                return $deliveryBodies;
            }

            self::assertSame([60, 60], $frame->method());
            $this->readFrame($listener, $client);
            $deliveryBodies[] = $this->readFrame($listener, $client)->payload;
        }

        self::fail('Timed out waiting for basic.cancel-ok.');
    }

    /**
     * @param resource $client
     */
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

    private function contentHeader(int $channel, int $bodySize, ?string $contentType = null, int $priority = 0): Frame
    {
        $flags = 0b0001000000000000 | 0b0000100000000000;
        $values = "\x02" . chr($priority);
        if ($contentType !== null) {
            $flags |= 0b1000000000000000;
            $values = $this->shortString($contentType) . $values;
        }

        return new Frame(Frame::TYPE_HEADER, $channel, pack('nn', 60, 0) . $this->packLongLong($bodySize) . pack('n', $flags) . $values);
    }

    /**
     * @param array<string, mixed> $properties
     */
    private function contentHeaderWithProperties(int $channel, int $bodySize, array $properties): Frame
    {
        $flags = 0;
        $values = '';

        if (isset($properties['content_type'])) {
            $flags |= 0b1000000000000000;
            $values .= $this->shortString((string) $properties['content_type']);
        }
        if (isset($properties['content_encoding'])) {
            $flags |= 0b0100000000000000;
            $values .= $this->shortString((string) $properties['content_encoding']);
        }
        if (isset($properties['headers']) && is_array($properties['headers'])) {
            $flags |= 0b0010000000000000;
            $values .= $this->fieldTable($properties['headers']);
        }
        if (isset($properties['delivery_mode'])) {
            $flags |= 0b0001000000000000;
            $values .= chr((int) $properties['delivery_mode']);
        }
        if (isset($properties['priority'])) {
            $flags |= 0b0000100000000000;
            $values .= chr((int) $properties['priority']);
        }
        if (isset($properties['correlation_id'])) {
            $flags |= 0b0000010000000000;
            $values .= $this->shortString((string) $properties['correlation_id']);
        }
        if (isset($properties['reply_to'])) {
            $flags |= 0b0000001000000000;
            $values .= $this->shortString((string) $properties['reply_to']);
        }
        if (isset($properties['expiration'])) {
            $flags |= 0b0000000100000000;
            $values .= $this->shortString((string) $properties['expiration']);
        }
        if (isset($properties['message_id'])) {
            $flags |= 0b0000000010000000;
            $values .= $this->shortString((string) $properties['message_id']);
        }
        if (isset($properties['timestamp'])) {
            $flags |= 0b0000000001000000;
            $values .= $this->packLongLong((int) $properties['timestamp']);
        }
        if (isset($properties['type'])) {
            $flags |= 0b0000000000100000;
            $values .= $this->shortString((string) $properties['type']);
        }
        if (isset($properties['user_id'])) {
            $flags |= 0b0000000000010000;
            $values .= $this->shortString((string) $properties['user_id']);
        }
        if (isset($properties['app_id'])) {
            $flags |= 0b0000000000001000;
            $values .= $this->shortString((string) $properties['app_id']);
        }
        if (isset($properties['cluster_id'])) {
            $flags |= 0b0000000000000100;
            $values .= $this->shortString((string) $properties['cluster_id']);
        }

        return new Frame(Frame::TYPE_HEADER, $channel, pack('nn', 60, 0) . $this->packLongLong($bodySize) . pack('n', $flags) . $values);
    }

    /**
     * @return array<string, mixed>
     */
    private function completeBasicProperties(int $timestamp): array
    {
        return [
            'content_type' => 'application/json',
            'content_encoding' => 'utf-8',
            'headers' => ['string' => 'value', 'int' => 27, 'bool' => true],
            'delivery_mode' => 2,
            'priority' => 5,
            'correlation_id' => 'correlation-0027',
            'reply_to' => 'reply.queue',
            'expiration' => '60000',
            'message_id' => 'message-0027',
            'timestamp' => $timestamp,
            'type' => 'fluxq.test.message',
            'user_id' => 'guest',
            'app_id' => 'fluxq-pika-test',
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function basicPropertiesFromHeader(Frame $frame): array
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader($frame->payload);
        $reader->readShort();
        $reader->readShort();
        $reader->readLongLong();
        $flags = $reader->readShort();
        $properties = [];

        if (($flags & 0b1000000000000000) !== 0) {
            $properties['content_type'] = $reader->readShortString();
        }
        if (($flags & 0b0100000000000000) !== 0) {
            $properties['content_encoding'] = $reader->readShortString();
        }
        if (($flags & 0b0010000000000000) !== 0) {
            $properties['headers'] = $reader->readTable();
        }
        if (($flags & 0b0001000000000000) !== 0) {
            $properties['delivery_mode'] = $reader->readOctet();
        }
        if (($flags & 0b0000100000000000) !== 0) {
            $properties['priority'] = $reader->readOctet();
        }
        if (($flags & 0b0000010000000000) !== 0) {
            $properties['correlation_id'] = $reader->readShortString();
        }
        if (($flags & 0b0000001000000000) !== 0) {
            $properties['reply_to'] = $reader->readShortString();
        }
        if (($flags & 0b0000000100000000) !== 0) {
            $properties['expiration'] = $reader->readShortString();
        }
        if (($flags & 0b0000000010000000) !== 0) {
            $properties['message_id'] = $reader->readShortString();
        }
        if (($flags & 0b0000000001000000) !== 0) {
            $properties['timestamp'] = $reader->readLongLong();
        }
        if (($flags & 0b0000000000100000) !== 0) {
            $properties['type'] = $reader->readShortString();
        }
        if (($flags & 0b0000000000010000) !== 0) {
            $properties['user_id'] = $reader->readShortString();
        }
        if (($flags & 0b0000000000001000) !== 0) {
            $properties['app_id'] = $reader->readShortString();
        }
        if (($flags & 0b0000000000000100) !== 0) {
            $properties['cluster_id'] = $reader->readShortString();
        }
        $reader->assertComplete();

        return $properties;
    }

    /**
     * @param array<string, mixed> $values
     */
    private function fieldTable(array $values): string
    {
        $payload = '';
        foreach ($values as $key => $value) {
            $payload .= $this->shortString((string) $key);
            if (is_bool($value)) {
                $payload .= 't' . chr($value ? 1 : 0);
            } elseif (is_int($value)) {
                $payload .= 'I' . pack('N', $value);
            } elseif (is_array($value)) {
                $payload .= 'F' . $this->fieldTable($value);
            } elseif ($value === null) {
                $payload .= 'V';
            } else {
                $payload .= 'S' . $this->longString((string) $value);
            }
        }

        return pack('N', strlen($payload)) . $payload;
    }

    private function shortString(string $value): string
    {
        return chr(strlen($value)) . $value;
    }

    private function longString(string $value): string
    {
        return pack('N', strlen($value)) . $value;
    }

    private function startOk(?string $response = null): string
    {
        $response ??= "\0guest\0guest";

        return pack('N', 0)
            . $this->shortString('PLAIN')
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

    private function packLongLong(int $value): string
    {
        return pack('NN', intdiv($value, 4294967296), $value % 4294967296);
    }

    private function deliveryTagFromDeliver(Frame $frame): int
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShortString();

        return $reader->readLongLong();
    }

    private function consumerTagFromDeliver(Frame $frame): string
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));

        return $reader->readShortString();
    }

    private function consumerTagFromBasicCancelOk(Frame $frame): string
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));

        return $reader->readShortString();
    }

    private function consumerTagFromBasicCancel(Frame $frame): string
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));

        return $reader->readShortString();
    }

    private function deliveryTagFromBasicAck(Frame $frame): int
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));

        return $reader->readLongLong();
    }

    private function deliveryTagFromBasicGetOk(Frame $frame): int
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));

        return $reader->readLongLong();
    }

    private function redeliveredFromBasicGetOk(Frame $frame): bool
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));
        $reader->readLongLong();

        return ($reader->readOctet() & 0b00000001) !== 0;
    }

    private function queueDeclareMessageCount(Frame $frame): int
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShortString();

        return $reader->readLong();
    }

    private function queueDeclareName(Frame $frame): string
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));

        return $reader->readShortString();
    }

    private function queueDeclareConsumerCount(Frame $frame): int
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShortString();
        $reader->readLong();

        return $reader->readLong();
    }

    /**
     * @return array{reply_code: int, exchange: string, routing_key: string}
     */
    private function basicReturnDetails(Frame $frame): array
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));
        $replyCode = $reader->readShort();
        $reader->readShortString();
        $exchange = $reader->readShortString();
        $routingKey = $reader->readShortString();

        return [
            'reply_code' => $replyCode,
            'exchange' => $exchange,
            'routing_key' => $routingKey,
        ];
    }

    private function basicReturnReplyText(Frame $frame): string
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));
        $reader->readShort();

        return $reader->readShortString();
    }

    private function multipleFromBasicAck(Frame $frame): bool
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader(substr($frame->payload, 4));
        $reader->readLongLong();

        return ($reader->readOctet() & 0b00000001) !== 0;
    }

    private function bodySizeFromHeader(Frame $frame): int
    {
        $reader = new \FluxQ\Protocol\Amqp\AmqpMethodReader($frame->payload);
        $reader->readShort();
        $reader->readShort();

        return $reader->readLongLong();
    }

    private function replyCode(Frame $frame): int
    {
        $value = unpack('nreplyCode', substr($frame->payload, 4, 2));

        return (int) $value['replyCode'];
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
        $frame = $this->readFrame($listener, $client);
        self::assertSame(Frame::TYPE_METHOD, $frame->type);

        return $frame;
    }

    /**
     * @param resource $client
     */
    private function readFrame(AmqpListener $listener, mixed $client): Frame
    {
        if ($this->pendingFrames !== []) {
            return array_shift($this->pendingFrames);
        }

        $codec = new FrameCodec();

        for ($attempt = 0; $attempt < 200; $attempt++) {
            $listener->tick();
            $bytes = fread($client, 8192);
            if (is_string($bytes) && $bytes !== '') {
                $frames = $codec->push($bytes);
                if ($frames !== []) {
                    $this->pendingFrames = array_slice($frames, 1);

                    return $frames[0];
                }
            }
            usleep(1000);
        }

        self::fail('Timed out waiting for AMQP frame.');
    }

    private function singleDelivery(string $queue): \FluxQ\Broker\Delivery
    {
        $deliveries = $this->deliveriesForQueue($queue);
        self::assertCount(1, $deliveries);

        return $deliveries[0];
    }

    private function queueExists(string $queue): bool
    {
        return (new DestinationRepository($this->connection))->findByName($this->virtualHostId, $queue) !== null;
    }

    /**
     * @return list<DeliveryState>
     */
    private function deliveryStates(string $queue): array
    {
        return array_map(
            static fn (\FluxQ\Broker\Delivery $delivery): DeliveryState => $delivery->state,
            $this->deliveriesForQueue($queue)
        );
    }

    /**
     * @return list<\FluxQ\Broker\Delivery>
     */
    private function deliveriesForQueue(string $queue): array
    {
        $destination = (new DestinationRepository($this->connection))->findByName($this->virtualHostId, $queue);
        self::assertNotNull($destination);
        $subscription = (new SubscriptionRepository($this->connection))->findByName($destination->id, 'amqp');
        self::assertNotNull($subscription);

        return (new DeliveryRepository($this->connection))->allBySubscription($subscription->id);
    }

    private function payloadForSingleMessage(string $queue): string
    {
        return $this->messageForSingleDelivery($queue)->payload;
    }

    private function messageForSingleDelivery(string $queue): \FluxQ\Broker\Message
    {
        $delivery = $this->singleDelivery($queue);
        $route = (new MessageRouteRepository($this->connection))->findById($delivery->messageRouteId);
        self::assertNotNull($route);
        $message = (new MessageRepository($this->connection))->findById($route->messageId);
        self::assertNotNull($message);

        return $message;
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
            new MessageRouteRepository($this->connection),
            new MessageRepository($this->connection),
            $limits
        );
    }

    private function authenticator(): Authenticator
    {
        return new Authenticator(new UserRepository($this->connection));
    }

    private function authorizer(): Authorizer
    {
        return new Authorizer(new UserRepository($this->connection));
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
