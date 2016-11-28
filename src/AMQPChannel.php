<?php
namespace DataProcessors\AMQP;

use Icicle\Socket\Client\Client;
use Icicle\Awaitable\Deferred;
use Icicle\Coroutine\Coroutine;
use DataProcessors\AMQP\Constants\ClassTypes;
use DataProcessors\AMQP\Constants\TxMethods;
use DataProcessors\AMQP\Constants\ChannelMethods;
use DataProcessors\AMQP\Constants\ExchangeMethods;
use DataProcessors\AMQP\Constants\QueueMethods;
use DataProcessors\AMQP\Constants\BasicMethods;

class AMQPChannel
{
    /** @var AMQPConnection */
    protected $connection;

    /** @var int **/
    protected $channel_id;

    /** @var Protocol091 */
    protected $protocolWriter;

    /** @var AMQPBufferReader */
    protected $bufferReader;

    /** @var AMQPMessage */
    protected $pendingBasicDeliver;

    protected $callbacks = array();

    /** @var bool */
    protected $is_open = false;

    /** @var Deferred */
    protected $basic_get_deferred = null;

    /** @var AMQPMessage */
    protected $pendingBasicGet;

    /** @var AMQPMessage */
    protected $pendingBasicReturn;

    /** @var array */
    protected $waitQueue = array();

    /** @var bool */
    public $pendingClose = false;

    /** @var callable */
    private $onClosing;

    /** @var callable */
    private $onClosed;

    public function __construct(AMQPConnection $connection, int $channel_id)
    {
        $this->connection = $connection;
        $this->channel_id = $channel_id;
        $this->protocolWriter = new Protocol091();
        $this->bufferReader = new AMQPBufferReader();
    }

    /**
     * Set onClosing event handler
     *
     * @param callable $onClosing
     */
    public function setOnClosingHandler(callable $onClosing)
    {
        $this->onClosing = $onClosing;
    }

    /**
     * Set onClosed event handler
     *
     * @param callable $onClosing
     */
    public function setOnClosedHandler(callable $onClosed)
    {
        $this->onClosed = $onClosed;
    }

    /**
     * Open the channel
     *
     */
    public function open()
    {
        try {
            if ($this->is_open) {
                return null;
            }

            yield $this->sendMethodFrame($this->protocolWriter->channelOpen());
            
            $deferred = new Deferred();
            $this->add_wait([['class_id'=>ClassTypes::CHANNEL, 'method_id'=>ChannelMethods::OPEN_OK]], $deferred, null, null);
            yield $deferred->getPromise();
        } catch (\Exception $e) {
            yield $this->close();
            throw $e;
        }
    }

    /**
     * @param array $frame
     * @return \Generator
     */
    protected function sendMethodFrame(array $frame): \Generator
    {
        return $this->connection->sendChannelMethodFrame($this->channel_id, $frame);
    }

    /**
     * Enables/disables flow from peer
     *
     * @param bool $active
     */
    public function flow(bool $active)
    {
        yield $this->sendMethodFrame($this->protocolWriter->channelFlow($active));

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::CHANNEL, 'method_id'=>ChannelMethods::FLOW_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Direct access to a queue
     *
     * @param string $queue
     * @param bool $no_ack
     * @return mixed
     */
    public function basic_get(string $queue = '', bool $no_ack = false)
    {
        yield $this->sendMethodFrame($this->protocolWriter->basicGet(0, $queue, $no_ack));

        $this->basic_get_deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::BASIC, 'method_id'=>BasicMethods::GET_OK], ['class_id'=>ClassTypes::BASIC, 'method_id'=>BasicMethods::GET_EMPTY]], null, null, null);
        yield $this->basic_get_deferred->getPromise();
    }

    /**
     * Declares exchange
     *
     * @param string $exchange
     * @param string $type
     * @param bool $passive
     * @param bool $durable
     * @param bool $nowait
     * @param array $arguments
     */
    public function exchange_declare(
        string $exchange,
        string $type,
        bool $passive = false,
        bool $durable = false,
        bool $nowait = false,
        array $arguments = array()
    ) {
        yield $this->sendMethodFrame($this->protocolWriter->exchangeDeclare(0, $exchange, $type, $passive, $durable, false, false, $nowait, $arguments));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::EXCHANGE, 'method_id'=>ExchangeMethods::DECLARE_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Deletes an exchange
     *
     * @param string $exchange
     * @param bool $if_unused
     * @param bool $nowait
     */
    public function exchange_delete(string $exchange, bool $if_unused = false, bool $nowait = false)
    {
        yield $this->sendMethodFrame($this->protocolWriter->exchangeDelete(0, $exchange, $if_unused, $nowait));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::EXCHANGE, 'method_id'=>ExchangeMethods::DELETE_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Binds queue to an exchange
     *
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param bool $nowait
     * @param array $arguments
     */
    public function queue_bind(string $queue, string $exchange, string $routing_key = '', bool $nowait = false, array $arguments = array())
    {
        yield $this->sendMethodFrame($this->protocolWriter->queueBind(0, $queue, $exchange, $routing_key, $nowait, $arguments));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::QUEUE, 'method_id'=>QueueMethods::BIND_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Unbind queue from an exchange
     *
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param array $arguments
     */
    public function queue_unbind(string $queue, string $exchange, string $routing_key = '', array $arguments = array())
    {
        yield $this->sendMethodFrame($this->protocolWriter->queueUnbind(0, $queue, $exchange, $routing_key, $arguments));

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::QUEUE, 'method_id'=>QueueMethods::UNBIND_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Declares queue, creates if needed
     *
     * @param string $queue
     * @param bool $passive
     * @param bool $durable
     * @param bool $exclusive
     * @param bool $auto_delete
     * @param bool $nowait
     * @param array $arguments
     */
    public function queue_declare(
        string $queue = '',
        bool $passive = false,
        bool $durable = false,
        bool $exclusive = false,
        bool $auto_delete = true,
        bool $nowait = false,
        array $arguments = array()
    ) {
        yield $this->sendMethodFrame($this->protocolWriter->queueDeclare(0, $queue, $passive, $durable, $exclusive, $auto_delete, $nowait, $arguments));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::QUEUE, 'method_id'=>QueueMethods::DECLARE_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Deletes a queue
     *
     * @param string $queue
     * @param bool $if_unused
     * @param bool $if_empty
     * @param bool $nowait
     */
    public function queue_delete(string $queue = '', bool $if_unused = false, bool $if_empty = false, bool $nowait = false)
    {
        yield $this->sendMethodFrame($this->protocolWriter->queueDelete(0, $queue, $if_unused, $if_empty, $nowait));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::QUEUE, 'method_id'=>QueueMethods::DELETE_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Purges a queue
     *
     * @param string $queue
     * @param bool $nowait
     */
    public function queue_purge(string $queue = '', bool $nowait = false)
    {
        yield $this->sendMethodFrame($this->protocolWriter->queuePurge(0, $queue, $nowait));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::QUEUE, 'method_id'=>QueueMethods::PURGE_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Selects standard transaction mode
     */
    public function tx_select()
    {
        yield $this->sendMethodFrame($this->protocolWriter->txSelect());
        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::TX, 'method_id'=>TxMethods::SELECT_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Commit the current transaction
     */
    public function tx_commit()
    {
        yield $this->sendMethodFrame($this->protocolWriter->txCommit());
        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::TX, 'method_id'=>TxMethods::COMMIT_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Rollback the current transaction
     */
    public function tx_rollback()
    {
        yield $this->sendMethodFrame($this->protocolWriter->txRollback());
        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::TX, 'method_id'=>TxMethods::ROLLBACK_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Starts a queue consumer
     *
     * @param string $queue
     * @param string $consumer_tag
     * @param bool $no_local
     * @param bool $no_ack
     * @param bool $exclusive
     * @param bool $nowait
     * @param callback|null $callback
     * @param array $arguments
     */
    public function basic_consume(
        string $queue = '',
        string $consumer_tag = '',
        bool $no_local = false,
        bool $no_ack = false,
        bool $exclusive = false,
        bool $nowait = false,
        callable $callback = null,
        array $arguments = array()
    ) {
        yield $this->sendMethodFrame($this->protocolWriter->basicConsume(0, $queue, $consumer_tag, $no_local, $no_ack, $exclusive, $nowait, $arguments));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            if (!$consumer_tag) {
                throw new Exception\AMQPRuntimeException("consumer_tag must be specified if nowait is true");
            } else {
                $this->callbacks[$consumer_tag] = $callback;
            }
            yield $consumer_tag;
        } else {
            $deferred = new Deferred();
            $this->add_wait([['class_id'=>ClassTypes::BASIC, 'method_id'=>BasicMethods::CONSUME_OK]], $deferred, $consumer_tag, $callback);
            yield $deferred->getPromise();
        }
    }

    /**
     * Ends a queue consumer
     *
     * @param string $consumer_tag
     * @param bool $nowait
     */
    public function basic_cancel(string $consumer_tag, bool $nowait = false)
    {
        yield $this->sendMethodFrame($this->protocolWriter->basicCancel($consumer_tag, $nowait));

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::BASIC, 'method_id'=>BasicMethods::CANCEL_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }


    /**
     * Acknowledges one or more messages
     *
     * @param string $delivery_tag
     * @param bool $multiple
     */
    public function basic_ack(string $delivery_tag, bool $multiple = false)
    {
        yield $this->sendMethodFrame($this->protocolWriter->basicAck($delivery_tag, $multiple));
    }

    /**
     * Rejects an incoming message
     *
     * @param string $delivery_tag
     * @param bool $requeue
     */
    public function basic_reject(string $delivery_tag, bool $requeue)
    {
        yield $this->sendMethodFrame($this->protocolWriter->basicReject($delivery_tag, $requeue));
    }

    /**
     * Request a channel close
     *
     * @param int $reply_code
     * @param string $reply_text
     * @param int $class_id
     * @param int $method_id
     */
    public function close(int $reply_code = 0, string $reply_text = '', int $class_id = 0, int $method_id = 0)
    {
        if ($this->is_open !== true || null === $this->connection) {
            $this->doClose();
            return; // already closed
        }

        if (null !== $this->onClosing) {
            ($this->onClosing)();
        }

        yield $this->sendMethodFrame($this->protocolWriter->channelClose($reply_code, $reply_text, $class_id, $method_id));

        $this->pendingClose = true;

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::CHANNEL, 'method_id'=>ChannelMethods::CLOSE_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Tear down this object, after we've agreed to close with the server.
     */
    protected function doClose()
    {
        $this->channel_id = null;
        $this->connection = null;
        $this->is_open = false;

        if (null !== $this->onClosed) {
            ($this->onClosed)();
        }
    }

    public function isClosed()
    {
        return ($this->is_open === false) && ($this->channel_id === null) && ($this->connection === null);
    }

    /**
     * Publishes a message
     *
     * @param AMQPMessage $msg
     * @param string $exchange
     * @param string $routing_key
     * @param bool $mandatory
     * @param bool $immediate
     * @return \Generator
     */
    public function basic_publish(
        AMQPMessage $msg,
        string $exchange = '',
        string $routing_key = '',
        bool $mandatory = false,
        bool $immediate = false
    ): \Generator {
        if ($this->connection === null) {
            throw new Exception\AMQPConnectionException("AMQP Connection is closed");
        }

        $data = chr(1); // METHOD frame type
        $data .= pack('n', $this->channel_id);
        $data .= pack('N', strlen($exchange) + strlen($routing_key) + 9);
        $data .= "\x00\x3C\x00\x28\x00\x00"; // BASIC; PUBLISH; zeros
        $data .= chr(strlen($exchange));
        $data .= $exchange;
        $data .= chr(strlen($routing_key));
        $data .= $routing_key;
        $bits = ($mandatory ? 1 : 0) | ($immediate ? 2 : 0);
        $data .= chr($bits);
        $data .= "\xCE\x02"; // FRAME END; HEADER frame type

        ////////// HEADER //////////
        $packed_properties = $msg->serializeProperties();
        $data .= pack('n', $this->channel_id);
        $data .= pack('N', strlen($packed_properties) + 12);
        $data .= "\x00\x3C\x00\x00"; // BASIC; 0
        $data .= pack('J', $msg->body_size);
        $data .= $packed_properties;

        ////////// BODY //////////
        if ($msg->body_size <= $this->connection->frame_max - 8) { // payload fits in single body frame
            $data .= "\xCE\x03"; // FRAME END; BODY FRAME type
            $data .= pack('n', $this->channel_id);
            $data .= pack('N', $msg->body_size);
            $data .= $msg->body;
            $data .= "\xCE"; // FRAME END
        } else { // multiple body frames
            $data .= "\xCE"; // FRAME END
            $position = 0;
            while ($position < $msg->body_size) {
                $payload = substr($msg->body, $position, $this->connection->frame_max - 8);
                $position += $this->connection->frame_max - 8;
                $data .= "\x03"; // BODY FRAME type
                $data .= pack('n', $this->channel_id);
                $data .= pack('N', strlen($payload));
                $data .= $payload;
                $data .= "\xCE"; // FRAME END
            }
        }
        return $this->connection->sendRaw($data);
    }

    /**
    * Specifies QoS
    *
    * @param int $prefetch_size
    * @param int $prefetch_count
    * @param bool $a_global
    * @return mixed
    */
    public function basic_qos(int $prefetch_size, int $prefetch_count, bool $a_global)
    {
        yield $this->sendMethodFrame($this->protocolWriter->basicQos($prefetch_size, $prefetch_count, $a_global));

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::BASIC, 'method_id'=>BasicMethods::QOS_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Redelivers unacknowledged messages
     *
     * @param bool $requeue
     */
    public function basic_recover(bool $requeue = false)
    {
        yield $this->sendMethodFrame($this->protocolWriter->basicRecover($requeue));

        $deferred = new Deferred();
        $this->add_wait([['class_id'=>ClassTypes::BASIC, 'method_id'=>BasicMethods::RECOVER_OK]], $deferred, null, null);
        yield $deferred->getPromise();
    }

    /**
     * Sets callback for basic_return
     *
     * @param  callable $callback
     * @throws \InvalidArgumentException if $callback is not callable
     */
    public function set_return_listener($callback)
    {
        if (false === is_callable($callback)) {
            throw new \InvalidArgumentException(sprintf(
                'Given callback "%s" should be callable. %s type was given.',
                $callback,
                gettype($callback)
            ));
        }

        $this->callbacks['__basic_return'] = $callback;
    }

    /**
     * Sets callback for channel_flow
     *
     * @param  callable $callback
     * @throws \InvalidArgumentException if $callback is not callable
     */
    public function set_flow_listener($callback)
    {
        if (false === is_callable($callback)) {
            throw new \InvalidArgumentException(sprintf(
                'Given callback "%s" should be callable. %s type was given.',
                $callback,
                gettype($callback)
            ));
        }

        $this->callbacks['__channel_flow'] = $callback;
    }

    public function isWaitFrame(int $frame_type, string $payload)
    {
        if ($frame_type != 1) {
            throw new Exception\AMQPRuntimeException(sprintf(
                'Expecting AMQP method, received frame type: %s (%s)',
                $frame_type,
                Constants091::$FRAME_TYPES[$frame_type]
            ));
        }

        if (strlen($payload) < 4) {
            throw new Exception\AMQPOutOfBoundsException('Method frame too short');
        }

        if (count($this->waitQueue) > 0) {
            $looking_for_methods = $this->waitQueue[0]['methods'];
            list(, $class_id, $method_id) = unpack('n2', substr($payload, 0, 4));

            foreach ($looking_for_methods as $looking_for_method) {
                if (($class_id == $looking_for_method['class_id']) && ($method_id == $looking_for_method['method_id'])) {
                    if (($class_id == ClassTypes::BASIC) && ($method_id == BasicMethods::CONSUME_OK)) {
                        $this->basic_consume_ok($payload);
                    } elseif (($class_id == ClassTypes::QUEUE) && ($method_id == QueueMethods::DECLARE_OK)) {
                        $this->queue_declare_ok($payload);
                    } elseif (($class_id == ClassTypes::CHANNEL) && ($method_id == ChannelMethods::FLOW_OK)) {
                        $this->channel_flow_ok($payload);
                    } elseif (($class_id == ClassTypes::QUEUE) && ($method_id == QueueMethods::PURGE_OK)) {
                        $this->queue_purge_ok($payload);
                    } elseif (($class_id == ClassTypes::QUEUE) && ($method_id == QueueMethods::DELETE_OK)) {
                        $this->queue_delete_ok($payload);
                    } else {
                        if (($class_id == ClassTypes::CHANNEL) && ($method_id == ChannelMethods::OPEN_OK)) {
                            $this->is_open = true;
                        } elseif (($class_id == ClassTypes::CHANNEL) && ($method_id == ChannelMethods::CLOSE_OK)) {
                            $this->doClose();
                        } elseif (($class_id == ClassTypes::BASIC) && ($method_id == BasicMethods::GET_OK)) {
                            $this->basicGetOk($payload);
                        } elseif (($class_id == ClassTypes::BASIC) && ($method_id == BasicMethods::CANCEL_OK)) {
                            $this->basicCancelOk($payload);
                        } elseif (($class_id == ClassTypes::BASIC) && ($method_id == BasicMethods::GET_EMPTY)) {
                            $this->basicGetEmpty($payload);
                        }
                        //do nothing for: exchange.declare_ok, exchange.delete_ok, queue.bind_ok, queue.unbind_ok, tx.select_ok, tx.commit_ok, tx.rollback_ok, basic.qos_ok, basic.recover_ok
                        //
                        if ($this->waitQueue[0]['deferred'] !== null) {
                            $this->waitQueue[0]['deferred']->resolve();
                        }
                    }
                    unset($this->waitQueue[0]);
                    $this->waitQueue = array_values($this->waitQueue);
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    /**
    * Add to the wait queue
    *
    * @param array $methods
    * @param Deferred $deferred
    * @param string $consumer_tag
    * @param Callable $callback
    */
    public function add_wait(array $methods, Deferred $deferred = null, string $consumer_tag = null, callable $callback = null)
    {
        $this->waitQueue[] = array('methods'=>$methods, 'deferred'=>$deferred, 'consumer_tag'=>$consumer_tag, 'callback'=>$callback);
    }

    /**
     * Basic consume ok
     *
     * @param string payload
     */
    protected function basic_consume_ok(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $consumer_tag = $bufferReader->read_shortstr();
        $this->callbacks[$consumer_tag] = $this->waitQueue[0]['callback'];
        if ($this->waitQueue[0]['deferred'] !== null) {
            $this->waitQueue[0]['deferred']->resolve($consumer_tag);
        }
    }

    /**
     * Queue declare ok
     *
     * @param string payload
     */
    protected function queue_declare_ok(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $queue = $bufferReader->read_shortstr();
        $message_count = $bufferReader->read_long();
        $consumer_count = $bufferReader->read_long();
        if ($this->waitQueue[0]['deferred'] !== null) {
            $this->waitQueue[0]['deferred']->resolve([$queue, $message_count, $consumer_count]);
        }
    }

    /**
     * Queue purge ok
     *
     * @param string payload
     */
    protected function queue_purge_ok(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $message_count = $bufferReader->read_long();
        if ($this->waitQueue[0]['deferred'] !== null) {
            $this->waitQueue[0]['deferred']->resolve($message_count);
        }
    }

    /**
     * Queue delete ok
     *
     * @param string payload
     */
    protected function queue_delete_ok(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $message_count = $bufferReader->read_long();
        if ($this->waitQueue[0]['deferred'] !== null) {
            $this->waitQueue[0]['deferred']->resolve($message_count);
        }
    }

    /**
     * Basic cancel ok
     *
     * @param string payload
     */
    protected function basicCancelOk(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $consumer_tag = $bufferReader->read_shortstr();
        unset($this->callbacks[$consumer_tag]);
    }

    /**
     * Channel flow ok
     *
     * @param string payload
     */
    protected function channel_flow_ok(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $active = $bufferReader->read_bit();
        if ($this->waitQueue[0]['deferred'] !== null) {
            $this->waitQueue[0]['deferred']->resolve($active);
        }
    }

    /**
     * Flow (received from server)
     *
     * @param string payload
     */
    public function serverFlow(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $active = $bufferReader->read_bit();
        if (isset($this->callbacks['__channel_flow'])) {
            $this->callbacks['__channel_flow']($active);
            //
            yield $this->sendMethodFrame($this->protocolWriter->channelFlowOk($active));
        }
    }

    /**
     * Basic deliver
     *
     * @param string payload
     */
    public function basicDeliver(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $this->bufferReader->reuse($args);
        $msg = new AMQPMessage();
        $this->pendingBasicDeliver = $msg;
        $msg->delivery_info = array(
            'channel' => $this,
            'consumer_tag' => $this->bufferReader->read_shortstr(),
            'delivery_tag' => $this->bufferReader->read_longlong(),
            'redelivered' => $this->bufferReader->read_bit(),
            'exchange' => $this->bufferReader->read_shortstr(),
            'routing_key' => $this->bufferReader->read_shortstr()
        );
    }

    /**
     * Returns a failed message
     *
     * @param string payload
     */
    public function basicReturn(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $this->bufferReader->reuse($args);
        $msg = new AMQPMessage();
        $this->pendingBasicReturn = $msg;
        $msg->delivery_info = [
            'channel' => $this,
            'reply_code' => $this->bufferReader->read_short(),
            'reply_text' => $this->bufferReader->read_shortstr(),
            'exchange' => $this->bufferReader->read_shortstr(),
            'routing_key' => $this->bufferReader->read_shortstr()
        ];
    }

    /**
     * Basic get ok
     *
     * @param string payload
     */
    protected function basicGetOk(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $this->bufferReader->reuse($args);
        $msg = new AMQPMessage();
        $this->pendingBasicGet = $msg;
        $msg->delivery_info = array(
            'channel' => $this,
            'delivery_tag' => $this->bufferReader->read_longlong(),
            'redelivered' => $this->bufferReader->read_bit(),
            'exchange' => $this->bufferReader->read_shortstr(),
            'routing_key' => $this->bufferReader->read_shortstr(),
            'message_count' => $this->bufferReader->read_long()
        );
    }

    /**
     * Basic get empty
     *
     * @param string payload
     */
    protected function basicGetEmpty(string $payload)
    {
        unset($this->pendingBasicGet);
        if (isset($this->basic_get_deferred)) {
            $this->basic_get_deferred->resolve(null);
            unset($this->basic_get_deferred);
        }
    }

    /**
     * Process a FRAME-HEADER
     *
     * @param string payload
     * @throws Exception\AMQPRuntimeException if we weren't expecting the frame header
     */
    public function frameHeader(string $payload)
    {
        if (isset($this->pendingBasicDeliver)) {
            $msg = $this->pendingBasicDeliver;
        } elseif (isset($this->pendingBasicGet)) {
            $msg = $this->pendingBasicGet;
        } elseif (isset($this->pendingBasicReturn)) {
            $msg = $this->pendingBasicReturn;
        } else {
            throw new Exception\AMQPRuntimeException("Unexpected FRAME-HEADER");
        }
        //
        $this->bufferReader->reuse(substr($payload, 0, 12));
        $class_id = $this->bufferReader->read_short();
        $weight = $this->bufferReader->read_short();
        $msg->body_size = $this->bufferReader->read_longlong();
        //
        $this->bufferReader->reuse(substr($payload, 12, strlen($payload) - 12));
        $msg->loadProperties($this->bufferReader);
    }

    /**
     * Process a FRAME-BODY
     *
     * @param string payload
     * @throws Exception\AMQPRuntimeException if we weren't expecting the frame body
     */
    public function frameBody(string $payload)
    {
        if (isset($this->pendingBasicDeliver)) {
            $msg = $this->pendingBasicDeliver;
            //
            $msg->tmp_body_parts[] = $payload;
            $msg->body_received += strlen($payload);
            //
            if ($msg->body_received >= $msg->body_size) {
                $msg->body = implode('', $msg->tmp_body_parts);
                unset($msg->tmp_body_parts);
                unset($this->pendingBasicDeliver);
                if (isset($msg->delivery_info['consumer_tag'])) {
                    $consumer_tag = $msg->delivery_info['consumer_tag'];
                    if (isset($this->callbacks[$consumer_tag])) {
                        $this->callbacks[$consumer_tag]($msg);
                    }
                }
            }
        } elseif (isset($this->pendingBasicGet)) {
            $msg = $this->pendingBasicGet;
            //
            $msg->tmp_body_parts[] = $payload;
            $msg->body_received += strlen($payload);
            //
            if ($msg->body_received >= $msg->body_size) {
                $msg->body = implode('', $msg->tmp_body_parts);
                unset($msg->tmp_body_parts);
                unset($this->pendingBasicGet);
                if (isset($this->basic_get_deferred)) {
                    $this->basic_get_deferred->resolve($msg);
                    unset($this->basic_get_deferred);
                }
            }
        } elseif (isset($this->pendingBasicReturn)) {
            $msg = $this->pendingBasicReturn;
            //
            $msg->tmp_body_parts[] = $payload;
            $msg->body_received += strlen($payload);
            //
            if ($msg->body_received >= $msg->body_size) {
                $msg->body = implode('', $msg->tmp_body_parts);
                unset($msg->tmp_body_parts);
                unset($this->pendingBasicReturn);
                if (isset($this->callbacks['__basic_return'])) {
                    $this->callbacks['__basic_return']($msg);
                }
            }
        } else {
            throw new Exception\AMQPRuntimeException("Unexpected FRAME-BODY");
        }
    }
}
