<?php

namespace Icicle\Tests\AMQP\Functional;

use Icicle\AMQP\AMQPChannel;
use Icicle\AMQP\AMQPConnection;
use Icicle\AMQP\AMQPMessage;
use Icicle\Coroutine;
use Icicle\Loop;
use Icicle\Promise;

class PublishConsumeTest extends \PHPUnit_Framework_TestCase
{

    protected $exchange_name = 'test_exchange';

    protected $queue_name = null;

    protected $msg_body;

    public function setUp()
    {
        Loop\loop(new Loop\SelectLoop());
    }


    public function goTestChannelFlow() 
    {
        $conn = new AMQPConnection();
        yield $conn->connect(HOST, PORT, USER, PASS, VHOST);
        $channel = yield $conn->channel();
  
        // ensure flow status makes the round trip from server in channel.flow_ok response
        $active = yield $channel->flow(false);
        $this->assertEquals($active,false);

        $active = yield $channel->flow(true);
        $this->assertEquals($active,true);

        yield $channel->close();
        yield $conn->close();
    }

    
    public function testChannelFlow()
    {
        $coroutine = new Coroutine\Coroutine($this->goTestChannelFlow());
        $coroutine->done();
        Loop\run();  
    }


    public function goTestBasicReturn() 
    {
        $conn = new AMQPConnection();
        yield $conn->connect(HOST, PORT, USER, PASS, VHOST);
        $channel = yield $conn->channel();

        $msg = new AMQPMessage('test');

        $deferredDeliver = new Promise\Deferred();
        $channel->set_return_listener(function($msg) use (&$deferredDeliver) {
            $deferredDeliver->resolve();
          });

        // publish a 'mandatory' message that should get returned because it could not be routed to a queue
        yield $channel->basic_publish($msg,'','non_existent_routing_key',true);

        yield $deferredDeliver->getPromise()->timeout(5); // throws timeout exception if we didn't get a basic_return in 5 seconds

        yield $channel->close();
        yield $conn->close();
    }


    public function testBasicReturn()
    {
        $coroutine = new Coroutine\Coroutine($this->goTestBasicReturn());
        $coroutine->done();
        Loop\run();  
    }


    public function goTestBasicGet() 
    {
        $conn = new AMQPConnection();
        yield $conn->connect(HOST, PORT, USER, PASS, VHOST);
        $channel = yield $conn->channel();

        yield $channel->exchange_declare($this->exchange_name, 'direct', false, false, false);
        list($this->queue_name, ,) = yield $channel->queue_declare();

        yield $channel->queue_bind($this->queue_name, $this->exchange_name, $this->queue_name);

        $msg = new AMQPMessage('test');

        yield $channel->basic_publish($msg, $this->exchange_name, $this->queue_name);
        $get_msg = yield $channel->basic_get($this->queue_name);
        $this->assertEquals($get_msg->body, $msg->body);
        $get_msg = yield $channel->basic_get($this->queue_name);
        $this->assertEquals($get_msg,null); // basic_get returns empty when there are no messages in the queue

        // cleanup
        yield $channel->exchange_delete($this->exchange_name);
        yield $channel->close();
        yield $conn->close();
    }


    public function testBasicGet()
    {
        $coroutine = new Coroutine\Coroutine($this->goTestBasicGet());
        $coroutine->done();
        Loop\run();  
    }


    public function goTestTx() 
    {
        $conn = new AMQPConnection();
        yield $conn->connect(HOST, PORT, USER, PASS, VHOST);
        $channel = yield $conn->channel();

        yield $channel->exchange_declare($this->exchange_name, 'direct', false, false, false);
        list($this->queue_name, ,) = yield $channel->queue_declare();

        yield $channel->queue_bind($this->queue_name, $this->exchange_name, $this->queue_name);

        $msg = new AMQPMessage('test');

        yield $channel->tx_select(); // start transaction mode

        yield $channel->basic_publish($msg, $this->exchange_name, $this->queue_name);
        yield $channel->tx_rollback(); // rollback a message

        yield $channel->basic_publish($msg, $this->exchange_name, $this->queue_name);
        yield $channel->tx_commit(); // commit a message
        
        $count = yield $channel->queue_purge($this->queue_name);
        $this->assertEquals($count, 1); // there should only be one of the two messages in the queue when we purge

        // cleanup
        yield $channel->exchange_delete($this->exchange_name);
        yield $channel->close();
        yield $conn->close();
    }

    public function testTx()
    {
        $coroutine = new Coroutine\Coroutine($this->goTestTx());
        $coroutine->done();
        Loop\run();  
    }


    public function goTestPublishConsume() 
    {
        $conn = new AMQPConnection();
        yield $conn->connect(HOST, PORT, USER, PASS, VHOST);
        $channel = yield $conn->channel();

        yield $channel->exchange_declare($this->exchange_name, 'direct', false, false, false);
        list($this->queue_name, ,) = yield $channel->queue_declare();

        yield $channel->queue_bind($this->queue_name, $this->exchange_name, $this->queue_name);
        $this->msg_body = 'foo bar baz äëïöü';

        $msg = new AMQPMessage($this->msg_body, array(
            'content_type' => 'text/plain',
            'delivery_mode' => 1,
            'correlation_id' => 'my_correlation_id',
            'reply_to' => 'my_reply_to'
        ));

        yield $channel->basic_publish($msg, $this->exchange_name, $this->queue_name);

        $deferredDeliver = new Promise\Deferred();
        
        yield $channel->basic_consume(
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
        $msg = yield $deferredDeliver->getPromise()->timeout(5);
        yield $this->process_msg($msg);

        // cleanup
        yield $channel->exchange_delete($this->exchange_name);
        yield $channel->close();
        $this->assertEquals($channel->isClosed(), true);
        yield $conn->close();
    }


    public function testPublishConsume() 
    {
        $coroutine = new Coroutine\Coroutine($this->goTestPublishConsume());
        $coroutine->done();
        Loop\run();
    }


    public function goTestQueueUnbindDelete() 
    {
        $conn = new AMQPConnection();
        yield $conn->connect(HOST, PORT, USER, PASS, VHOST);
        $channel = yield $conn->channel();

        yield $channel->exchange_declare($this->exchange_name, 'direct', false, false, false);
        list($this->queue_name, ,) = yield $channel->queue_declare();

        $msg = new AMQPMessage('test');

        yield $channel->queue_bind($this->queue_name, $this->exchange_name, $this->queue_name);
        yield $channel->basic_publish($msg, $this->exchange_name, $this->queue_name);
        yield $channel->queue_unbind($this->queue_name, $this->exchange_name, $this->queue_name);
        yield $channel->basic_publish($msg, $this->exchange_name, $this->queue_name);

        $count = yield $channel->queue_delete($this->queue_name);
        $this->assertEquals($count,1); // there should be one published message in the queue

        yield $channel->exchange_delete($this->exchange_name);
        yield $channel->close();
        yield $conn->close();
    }


    public function testQueueUnbindDelete() 
    {
        $coroutine = new Coroutine\Coroutine($this->goTestQueueUnbindDelete());
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
