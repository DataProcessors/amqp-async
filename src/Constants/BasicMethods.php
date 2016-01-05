<?php
namespace DataProcessors\AMQP\Constants;

class BasicMethods
{
    const QOS = 10;
    const QOS_OK = 11;
    const CONSUME = 20;
    const CONSUME_OK = 21;
    const CANCEL = 30;
    const CANCEL_OK = 31;
    const PUBLISH = 40;
    const RETURN = 50;
    const DELIVER = 60;
    const GET = 70;
    const GET_OK = 71;
    const GET_EMPTY = 72;
    const ACK = 80;
    const REJECT = 90;
    const RECOVER_ASYNC = 100;
    const RECOVER = 110;
    const RECOVER_OK = 111;
    const NACK = 120;
}
