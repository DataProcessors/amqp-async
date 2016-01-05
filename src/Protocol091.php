<?php
namespace DataProcessors\AMQP;

use DataProcessors\AMQP\Constants\ClassTypes;
use DataProcessors\AMQP\Constants\TxMethods;
use DataProcessors\AMQP\Constants\ChannelMethods;
use DataProcessors\AMQP\Constants\ExchangeMethods;
use DataProcessors\AMQP\Constants\QueueMethods;
use DataProcessors\AMQP\Constants\BasicMethods;
use DataProcessors\AMQP\Constants\ConnectionMethods;

class Protocol091
{
   /**
     * @return array
     */
    public static function connectionStartOk(array $client_properties, string $mechanism, string $response, string $locale)
    {
        $args = new AMQPBufferWriter();
        $args->write_table($client_properties);
        $args->write_shortstr($mechanism);
        $args->write_longstr($response);
        $args->write_shortstr($locale);
        return [ClassTypes::CONNECTION, ConnectionMethods::START_OK, $args->getvalue()];

    }



    /**
     * @return array
     */
    public function connectionSecure($challenge)
    {
        $args = new AMQPBufferWriter();
        $args->write_longstr($challenge);
        return [ClassTypes::CONNECTION, ConnectionMethods::SECURE, $args->getvalue()];
    }



    /**
     * @return array
     */
    public static function connectionTuneOk(int $channel_max, int $frame_max, int $heartbeat)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($channel_max);
        $args->write_long($frame_max);
        $args->write_short($heartbeat);
        return [ClassTypes::CONNECTION, ConnectionMethods::TUNE_OK, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function connectionOpen($virtual_host = '/', $reserved1 = '', $reserved2 = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_shortstr($virtual_host);
        $args->write_shortstr($reserved1);
        $args->write_bits(array($reserved2));
        return [ClassTypes::CONNECTION, ConnectionMethods::OPEN, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function connectionClose($reply_code, $reply_text = '', $class_id, $method_id)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reply_code);
        $args->write_shortstr($reply_text);
        $args->write_short($class_id);
        $args->write_short($method_id);
        return [ClassTypes::CONNECTION, ConnectionMethods::CLOSE, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function channelOpen($reserved1 = '')
    {
        $args = new AMQPBufferWriter();
        $args->write_shortstr($reserved1);
        return [ClassTypes::CHANNEL, ChannelMethods::OPEN, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function channelFlow($active)
    {
        $args = new AMQPBufferWriter();
        $args->write_bits(array($active));
        return array(ClassTypes::CHANNEL, ChannelMethods::FLOW, $args->getvalue());
    }



    /**
     * @return array
     */
    public static function channelFlowOk($active)
    {
        $args = new AMQPBufferWriter();
        $args->write_bits(array($active));
        return array(ClassTypes::CHANNEL, ChannelMethods::FLOW_OK, $args->getvalue());
    }



    /**
     * @return array
     */
    public function channelClose($reply_code, $reply_text = '', $class_id, $method_id)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reply_code);
        $args->write_shortstr($reply_text);
        $args->write_short($class_id);
        $args->write_short($method_id);
        return [ClassTypes::CHANNEL, ChannelMethods::CLOSE, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function exchangeDeclare($reserved1 = 0, $exchange, $type = 'direct', $passive = false, $durable = false, $reserved2 = false, $reserved3 = false, $nowait = false, $arguments = array())
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($exchange);
        $args->write_shortstr($type);
        $args->write_bits(array($passive, $durable, $reserved2, $reserved3, $nowait));
        $args->write_table(empty($arguments) ? array() : $arguments);
        return [ClassTypes::EXCHANGE, ExchangeMethods::DECLARE, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function exchangeDelete($reserved1 = 0, $exchange, $if_unused = false, $nowait = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($exchange);
        $args->write_bits(array($if_unused, $nowait));
        return [ClassTypes::EXCHANGE, ExchangeMethods::DELETE, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function exchangeBind($reserved1 = 0, $destination, $source, $routing_key = '', $nowait = false, $arguments = array())
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($destination);
        $args->write_shortstr($source);
        $args->write_shortstr($routing_key);
        $args->write_bits(array($nowait));
        $args->write_table(empty($arguments) ? array() : $arguments);
        return [ClassTypes::EXCHANGE, ExchangeMethods::BIND, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function exchangeUnbind($reserved1 = 0, $destination, $source, $routing_key = '', $nowait = false, $arguments = array())
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($destination);
        $args->write_shortstr($source);
        $args->write_shortstr($routing_key);
        $args->write_bits(array($nowait));
        $args->write_table(empty($arguments) ? array() : $arguments);
        return [ClassTypes::EXCHANGE, ExchangeMethods::UNBIND, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function queueDeclare($reserved1 = 0, $queue = '', $passive = false, $durable = false, $exclusive = false, $auto_delete = false, $nowait = false, $arguments = array())
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($queue);
        $args->write_bits(array($passive, $durable, $exclusive, $auto_delete, $nowait));
        $args->write_table(empty($arguments) ? array() : $arguments);
        return array(ClassTypes::QUEUE, QueueMethods::DECLARE, $args->getvalue());
    }



    /**
     * @return array
     */
    public function queueBind($reserved1 = 0, $queue = '', $exchange, $routing_key = '', $nowait = false, $arguments = array())
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($queue);
        $args->write_shortstr($exchange);
        $args->write_shortstr($routing_key);
        $args->write_bits(array($nowait));
        $args->write_table(empty($arguments) ? array() : $arguments);
        return [ClassTypes::QUEUE, QueueMethods::BIND, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function queuePurge($reserved1 = 0, $queue = '', $nowait = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($queue);
        $args->write_bits(array($nowait));
        return [ClassTypes::QUEUE, QueueMethods::PURGE, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function queueDelete($reserved1 = 0, $queue = '', $if_unused = false, $if_empty = false, $nowait = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($queue);
        $args->write_bits(array($if_unused, $if_empty, $nowait));
        return [ClassTypes::QUEUE, QueueMethods::DELETE, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function queueUnbind($reserved1 = 0, $queue = '', $exchange, $routing_key = '', $arguments = array())
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($queue);
        $args->write_shortstr($exchange);
        $args->write_shortstr($routing_key);
        $args->write_table(empty($arguments) ? array() : $arguments);
        return [ClassTypes::QUEUE, QueueMethods::UNBIND, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicQos($prefetch_size = 0, $prefetch_count = 0, $global = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_long($prefetch_size);
        $args->write_short($prefetch_count);
        $args->write_bits(array($global));
        return [ClassTypes::BASIC, BasicMethods::QOS, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicConsume($reserved1 = 0, $queue = '', $consumer_tag = '', $no_local = false, $no_ack = false, $exclusive = false, $nowait = false, $arguments = array())
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($queue);
        $args->write_shortstr($consumer_tag);
        $args->write_bits(array($no_local, $no_ack, $exclusive, $nowait));
        $args->write_table(empty($arguments) ? array() : $arguments);
        return [ClassTypes::BASIC, BasicMethods::CONSUME, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicCancel($consumer_tag, $nowait = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_shortstr($consumer_tag);
        $args->write_bits(array($nowait));
        return [ClassTypes::BASIC, BasicMethods::CANCEL, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicPublish($reserved1 = 0, $exchange = '', $routing_key = '', $mandatory = false, $immediate = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($exchange);
        $args->write_shortstr($routing_key);
        $args->write_bits(array($mandatory, $immediate));
        return [ClassTypes::BASIC, BasicMethods::PUBLISH, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicGet($reserved1 = 0, $queue = '', $no_ack = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_short($reserved1);
        $args->write_shortstr($queue);
        $args->write_bits(array($no_ack));
        return array(ClassTypes::BASIC, BasicMethods::GET, $args->getvalue());
    }



    /**
     * @return array
     */
    public function basicAck($delivery_tag = 0, $multiple = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_longlong($delivery_tag);
        $args->write_bits(array($multiple));
        return [ClassTypes::BASIC, BasicMethods::ACK, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicReject($delivery_tag, $requeue = true)
    {
        $args = new AMQPBufferWriter();
        $args->write_longlong($delivery_tag);
        $args->write_bits(array($requeue));
        return [ClassTypes::BASIC, BasicMethods::REJECT, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicRecoverAsync($requeue = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_bits(array($requeue));
        return [ClassTypes::BASIC, BasicMethods::RECOVER_ASYNC, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function basicRecover($requeue = false)
    {
        $args = new AMQPBufferWriter();
        $args->write_bits(array($requeue));
        return [ClassTypes::BASIC, BasicMethods::RECOVER, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function txSelect()
    {
        $args = new AMQPBufferWriter();
        return [ClassTypes::TX, TxMethods::SELECT, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function txCommit()
    {
        $args = new AMQPBufferWriter();
        return [ClassTypes::TX, TxMethods::COMMIT, $args->getvalue()];
    }



    /**
     * @return array
     */
    public function txRollback()
    {
        $args = new AMQPBufferWriter();
        return [ClassTypes::TX, TxMethods::ROLLBACK, $args->getvalue()];
    }
}
