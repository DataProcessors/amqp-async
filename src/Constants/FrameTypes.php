<?php
namespace DataProcessors\AMQP\Constants;

class FrameTypes
{
    const METHOD = 1;
    const HEADER = 2;
    const BODY = 3;
    const HEARTBEAT = 8;
}
