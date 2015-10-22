<?php

namespace DataProcessors\AMQP\Tests\Functional;

use DataProcessors\AMQP\AMQPChannel;
use DataProcessors\AMQP\AMQPConnection;
use DataProcessors\AMQP\AMQPMessage;
use Icicle\Coroutine;
use Icicle\Loop;
use Icicle\Promise;

class PublishConsumeTest extends \PHPUnit_Framework_TestCase
{

    protected $exchange_name = 'test_exchange';

    protected $queue_name = null;

    protected $msg_body;

    /**
     * @var AMQPConnection
     */
    protected $conn;

    /**
     * @var AMQPChannel
     */
    protected $ch;



    public function setUp()
    {
        Loop\loop(new Loop\SelectLoop());
    }


    public function goTestPublishConsume() 
    {
        $this->conn = new AMQPConnection();
        yield $this->conn->connect(HOST, PORT, USER, PASS, VHOST);
        $this->ch = yield $this->conn->channel();

        yield $this->ch->exchange_declare($this->exchange_name, 'direct', false, false, false);
        list($this->queue_name, ,) = yield $this->ch->queue_declare();

        yield $this->ch->queue_bind($this->queue_name, $this->exchange_name, $this->queue_name);
        $this->msg_body = 'foo bar baz äëïöü';

        $msg = new AMQPMessage($this->msg_body, array(
            'content_type' => 'text/plain',
            'delivery_mode' => 1,
            'correlation_id' => 'my_correlation_id',
            'reply_to' => 'my_reply_to'
        ));

        yield $this->ch->basic_publish($msg, $this->exchange_name, $this->queue_name);

        $deferredDeliver = new Promise\Deferred();
        
        yield $this->ch->basic_consume(
            $this->queue_name,
            getmypid(),
            false,
            false,
            false,
            false,
            function ($msg) use ($deferredDeliver) {
              $deferredDeliver->resolve($msg);
            }
        );

        // wait for a message then continue processing
        $msg = yield $deferredDeliver->getPromise();
        yield $this->process_msg($msg);

        // cleanup
        yield $this->ch->exchange_delete($this->exchange_name);
        yield $this->ch->close();
        yield $this->conn->close();
    }


    public function testPublishConsume() 
    {
        $coroutine = new Coroutine\Coroutine($this->goTestPublishConsume());
        $coroutine->done();
        Loop\run();
    }


    public function process_msg($msg)
    {
        $delivery_info = $msg->delivery_info;

        yield $delivery_info['channel']->basic_ack($delivery_info['delivery_tag']);
        yield $delivery_info['channel']->basic_cancel($delivery_info['consumer_tag']);

        $this->assertEquals($this->msg_body, $msg->body);

        //delivery tests
        $this->assertEquals(getmypid(), $delivery_info['consumer_tag']);
        $this->assertEquals($this->queue_name, $delivery_info['routing_key']);
        $this->assertEquals($this->exchange_name, $delivery_info['exchange']);
        $this->assertEquals(false, $delivery_info['redelivered']);

        //msg property tests
        $this->assertEquals('text/plain', $msg->get('content_type'));
        $this->assertEquals('my_correlation_id', $msg->get('correlation_id'));
        $this->assertEquals('my_reply_to', $msg->get('reply_to'));

        try {
          $msg->get('no_property');
          $this->fail('OutOfBoundsException has not been raised.');
        } catch (\OutOfBoundsException $e) {
          // do nothing
        }
    }



    public function tearDown()
    {
    }
}
