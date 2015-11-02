<?php
namespace Icicle\AMQP;

use Icicle\Socket\Client\Client;
use Icicle\Promise\Deferred;
use Icicle\Coroutine\Coroutine;

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

    public function __construct(AMQPConnection $connection, int $channel_id)
    {
        $this->connection = $connection;
        $this->channel_id = $channel_id;
        $this->protocolWriter = new Protocol091();
        $this->bufferReader = new AMQPBufferReader();
    }

    /** 
     * Open the channel
     *
     */
    public function open(): \Generator
    {
        try {
            if ($this->is_open) {
                return null;
            }

            list($class_id, $method_id, $args) = $this->protocolWriter->channelOpen();
            yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
            $deferred = new Deferred();
            $this->add_wait(array('channel.open_ok'), $deferred, null, null);
            return $deferred->getPromise();
        } catch (\Exception $e) {
            yield $this->close();
            throw $e;
        }
    }

    /**
     * @param int $class_id
     * @param int $method_id
     * @param string $args
     */
    protected function send_method_frame(int $class_id, int $method_id, string $args = ''): \Generator
    {
        return $this->connection->send_channel_method_frame($this->channel_id, $class_id, $method_id, $args);
    }

    /**
     * Enables/disables flow from peer
     *
     * @param bool $active
     */
    public function flow(bool $active): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->channelFlow($active);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());

        $deferred = new Deferred();
        $this->add_wait(array('channel.flow_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Direct access to a queue
     *
     * @param string $queue
     * @param bool $no_ack
     */
    public function basic_get(string $queue = '', bool $no_ack = false): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->basicGet(0, $queue, $no_ack);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());

        $this->basic_get_deferred = new Deferred();
        $this->add_wait(array('basic.get_ok','basic.get_empty'), null, null, null);
        return $this->basic_get_deferred->getPromise();
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
    ): \Generator {
        list($class_id, $method_id, $args) = $this->protocolWriter->exchangeDeclare(0, $exchange, $type, $passive, $durable, false, false, $nowait, $arguments);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
       
        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait(array('exchange.declare_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Deletes an exchange
     *
     * @param string $exchange
     * @param bool $if_unused
     * @param bool $nowait
     */
    public function exchange_delete(string $exchange, bool $if_unused = false, bool $nowait = false): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->exchangeDelete(0,$exchange,$if_unused,$nowait);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
       
        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait(array('exchange.delete_ok'), $deferred, null, null);
        return $deferred->getPromise();
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
    public function queue_bind(string $queue, string $exchange, string $routing_key = '', bool $nowait = false, array $arguments = array()): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->queueBind(0,$queue,$exchange,$routing_key,$nowait,$arguments);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
        
        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait(array('queue.bind_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Unbind queue from an exchange
     *
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param array $arguments
     */
    public function queue_unbind(string $queue, string $exchange, string $routing_key = '', array $arguments = array()): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->queueUnbind(0,$queue,$exchange,$routing_key,$arguments);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
        
        $deferred = new Deferred();
        $this->add_wait(array('queue.unbind_ok'), $deferred, null, null);
        return $deferred->getPromise();
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
    ): \Generator {
        list($class_id, $method_id, $args) = $this->protocolWriter->queueDeclare(
            0,
            $queue,
            $passive,
            $durable,
            $exclusive,
            $auto_delete,
            $nowait,
            $arguments
        );
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
        
        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait(array('queue.declare_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Deletes a queue
     *
     * @param string $queue
     * @param bool $if_unused
     * @param bool $if_empty
     * @param bool $nowait
     */
    public function queue_delete(string $queue = '', bool $if_unused = false, bool $if_empty = false, bool $nowait = false): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->queueDelete(0,$queue,$if_unused,$if_empty,$nowait);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
        
        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait(array('queue.delete_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Purges a queue
     *
     * @param string $queue
     * @param bool $nowait
     */
    public function queue_purge(string $queue = '', bool $nowait = false): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->queuePurge(0, $queue, $nowait);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
        
        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait(array('queue.purge_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Selects standard transaction mode
     */
    public function tx_select(): \Generator
    {
        yield $this->send_method_frame(90, 10);
        $deferred = new Deferred();
        $this->add_wait(array('tx.select_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Commit the current transaction
     */
    public function tx_commit(): \Generator
    {
        yield $this->send_method_frame(90, 20);
        $deferred = new Deferred();
        $this->add_wait(array('tx.commit_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Rollback the current transaction
     */
    public function tx_rollback(): \Generator
    {
        yield $this->send_method_frame(90, 30);
        $deferred = new Deferred();
        $this->add_wait(array('tx.rollback_ok'), $deferred, null, null);
        return $deferred->getPromise();
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
        Callable $callback = null,
        array $arguments = array()
    ): \Generator {
        list($class_id, $method_id, $args) = $this->protocolWriter->basicConsume(
            0,
            $queue,
            $consumer_tag,
            $no_local,
            $no_ack,
            $exclusive,
            $nowait,
            $arguments
        );

        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());

        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            if (!$consumer_tag)
                throw new Exception\AMQPRuntimeException("consumer_tag must be specified if nowait is true");
            else
                $this->callbacks[$consumer_tag] = $callback;
            return $consumer_tag;
        }
        else {
            $deferred = new Deferred();
            $this->add_wait(array('basic.consume_ok'), $deferred, $consumer_tag, $callback);
            return $deferred->getPromise();
        }
    }

    /**
     * Ends a queue consumer
     *
     * @param string $consumer_tag
     * @param bool $nowait
     */
    public function basic_cancel(string $consumer_tag, bool $nowait = false): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->basicCancel($consumer_tag, $nowait);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
     
        if ($nowait) {
            /* no-wait: [do not send reply method] If set, the server will not respond to the method. The
                        client should not wait for a reply method. If the server could not complete the
                        method it will raise a channel or connection exception.
            */
            return;
        }

        $deferred = new Deferred();
        $this->add_wait(array('basic.cancel_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }


    /**
     * Acknowledges one or more messages
     *
     * @param string $delivery_tag
     * @param bool $multiple
     */
    public function basic_ack(string $delivery_tag, bool $multiple = false): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->basicAck($delivery_tag, $multiple);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
    }

    /**
     * Rejects an incoming message
     *
     * @param string $delivery_tag
     * @param bool $requeue
     */
    public function basic_reject(string $delivery_tag, bool $requeue): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->basicReject($delivery_tag, $requeue);
        yield $this->send_method_frame($class_id, $method_id, $args);
    }

    /**
     * Request a channel close
     *
     * @param int $reply_code
     * @param string $reply_text
     * @param int $class_id
     * @param int $method_id
     */
    public function close(int $reply_code = 0, string $reply_text = '', int $class_id = 0, int $method_id = 0): \Generator
    {
        if ($this->is_open !== true || null === $this->connection) {
            $this->do_close();
            return; // already closed
        }
        list($x_class_id, $x_method_id, $args) = $this->protocolWriter->channelClose(
            $reply_code,
            $reply_text,
            $class_id,
            $method_id
        );

        yield $this->send_method_frame($x_class_id, $x_method_id, $args->getvalue());

        $this->pendingClose = true;

        $deferred = new Deferred();
        $this->add_wait(array('channel.close_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Tear down this object, after we've agreed to close with the server.
     */
    protected function do_close()
    {
        $this->channel_id = null;
        $this->connection = null;
        $this->is_open = false;
    }

    public function isClosed(): bool
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
     */
    public function basic_publish(
        AMQPMessage $msg,
        string $exchange = '',
        string $routing_key = '',
        bool $mandatory = false,
        bool $immediate = false
    ): \Generator {
        list($class_id, $method_id, $args) = $this->protocolWriter->basicPublish(
            0,
            $exchange,
            $routing_key,
            $mandatory,
            $immediate
        );

        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
        yield $this->connection->send_channel_content(
            $this->channel_id,
            60,
            0,
            $msg->body_size,
            $msg->serialize_properties(),
            $msg->body);
    }

    /**
  	* Specifies QoS
  	*
  	* @param int $prefetch_size
  	* @param int $prefetch_count
  	* @param bool $a_global
  	*/
    public function basic_qos(int $prefetch_size, int $prefetch_count, bool $a_global): \Generator
    {
  	    list($class_id, $method_id, $args) = $this->protocolWriter->basicQos($prefetch_size,$prefetch_count,$a_global);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
  	    $deferred = new Deferred();
        $this->add_wait(array('basic.qos_ok'), $deferred, null, null);
        return $deferred->getPromise();
    }

    /**
     * Redelivers unacknowledged messages
     *
     * @param bool $requeue
     */
    public function basic_recover(bool $requeue = false): \Generator
    {
        list($class_id, $method_id, $args) = $this->protocolWriter->basicRecover($requeue);
        yield $this->send_method_frame($class_id, $method_id, $args->getvalue());
        $deferred = new Deferred();
        $this->add_wait(array('basic.recover_ok'), $deferred, null, null);
        return $deferred->getPromise();
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
            $method_sig_array = unpack('n2', substr($payload, 0, 4));
            $method_sig = $method_sig_array[1] . ',' . $method_sig_array[2];

            //echo "isWaitFrame looking for " . implode(",",$looking_for_methods) . " got " . Constants091::$GLOBAL_METHOD_NAMES[MiscHelper::methodSig($method_sig)] . "\n";

            foreach ($looking_for_methods as $looking_for_method) {
                if ($method_sig == Constants091::$GLOBAL_METHOD_SIGS[$looking_for_method]) {
                    if ($looking_for_method == 'basic.consume_ok') {
                        $this->basic_consume_ok($payload);
                    }
                    else
                    if ($looking_for_method == 'queue.declare_ok') {
                        $this->queue_declare_ok($payload);
                    }
                    else
                    if ($looking_for_method == 'channel.flow_ok') {
                        $this->channel_flow_ok($payload);
                    }
                    else
                    if ($looking_for_method == 'queue.purge_ok') {
                        $this->queue_purge_ok($payload);
                    }
                    else
                    if ($looking_for_method == 'queue.delete_ok') {
                        $this->queue_delete_ok($payload);
                    }
                    else {
                        if ($looking_for_method == 'channel.open_ok') {
                            $this->is_open = true;
                        }
                        else
                        if ($looking_for_method == 'channel.close_ok') {
                            $this->do_close();
                        }
                        else
                        if ($looking_for_method == 'basic.get_ok') {
                            $this->basic_get_ok($payload);
                        }
                        else
                        if ($looking_for_method == 'basic.cancel_ok') {
                            $this->basic_cancel_ok($payload);
                        }
                        else
                        if ($looking_for_method == 'basic.get_empty') {
                            $this->basic_get_empty($payload);
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
        }
        else
            return false;
    }

    /**
     * Add to the wait queue
     *
     * @param array $methods
     * @param Deferred $deferred
     * @param string $consumer_tag
     * @param Callable $callback
     */
    public function add_wait(array $methods, Deferred $deferred=null, string $consumer_tag=null, Callable $callback=null)
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
     * Queue delete ok
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
    protected function basic_cancel_ok(string $payload)
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
    public function server_flow(string $payload) 
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $bufferReader = new AMQPBufferReader($args);
        $active = $bufferReader->read_bit();
        if (isset($this->callbacks['__channel_flow'])) {
            $this->callbacks['__channel_flow']($active);
            //
            $args = new AMQPBufferWriter();
            $args->write_bits(array($active));
            yield $this->send_method_frame(20, 21, $args->getvalue()); // send channel.flow_ok response
        }
    }
    
    /**
     * Basic deliver
     *
     * @param string payload
     */
    public function basic_deliver(string $payload)
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
    public function basic_return(string $payload)
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
    public function basic_get_ok(string $payload)
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
    public function basic_get_empty(string $payload)
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
    public function frame_header(string $payload)
    {
        if (isset($this->pendingBasicDeliver)) {
            $msg = $this->pendingBasicDeliver;
        }
        else
        if (isset($this->pendingBasicGet)) {
            $msg = $this->pendingBasicGet;
        }
        else
        if (isset($this->pendingBasicReturn)) {
            $msg = $this->pendingBasicReturn;
        }
        else {
            throw new Exception\AMQPRuntimeException("Unexpected FRAME-HEADER");
        }
        //
        $this->bufferReader->reuse(substr($payload, 0, 12));
        $class_id = $this->bufferReader->read_short();
        $weight = $this->bufferReader->read_short();
        $msg->body_size = $this->bufferReader->read_longlong();
        //
        $this->bufferReader->reuse(substr($payload, 12, strlen($payload) - 12));
        $msg->load_properties($this->bufferReader);
    }

    /**
     * Process a FRAME-BODY
     *
     * @param string payload
     * @throws Exception\AMQPRuntimeException if we weren't expecting the frame body
     */
    public function frame_body(string $payload)
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
        }
        else
        if (isset($this->pendingBasicGet)) {
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
        }
        else
        if (isset($this->pendingBasicReturn)) {
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
        }
        else {
            throw new Exception\AMQPRuntimeException("Unexpected FRAME-BODY");
        }
    }

}