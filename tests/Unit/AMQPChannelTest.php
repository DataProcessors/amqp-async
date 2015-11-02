<?php

namespace Icicle\Tests\AMQP\Unit\Channel;

use Icicle\AMQP\AMQPChannel;

class AMQPChannelTest extends \PHPUnit_Framework_TestCase
{
    public function testCloseDoesNotEmitUndefinedPropertyWarningWhenSomeMethodsAreMocked()
    {
        $mockChannel = $this->getMockBuilder('\Icicle\AMQP\AMQPChannel')
            ->setMethods(array('queue_bind'))
            ->disableOriginalConstructor()
            ->getMock();
        /* @var $mockChannel \Icicle\AMQP\AMQPChannel */

        $mockChannel->close();
    }
}
