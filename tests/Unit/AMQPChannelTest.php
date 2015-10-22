<?php

namespace DataProcessors\AMQP\Tests\Unit\Channel;

use DataProcessors\AMQP\AMQPChannel;

class AMQPChannelTest extends \PHPUnit_Framework_TestCase
{
    public function testCloseDoesNotEmitUndefinedPropertyWarningWhenSomeMethodsAreMocked()
    {
        $mockChannel = $this->getMockBuilder('\DataProcessors\AMQP\AMQPChannel')
            ->setMethods(array('queue_bind'))
            ->disableOriginalConstructor()
            ->getMock();
        /* @var $mockChannel \PhpAmqpLib\Channel\AMQPChannel */

        $mockChannel->close();
    }
}
