<?php
namespace DataProcessors\AMQP\Constants;

class ConnectionMethods
{
    const START = 10;
    const START_OK = 11;
    const SECURE = 20;
    const SECURE_OK = 21;
    const TUNE = 30;
    const TUNE_OK = 31;
    const OPEN = 40;
    const OPEN_OK = 41;
    const CLOSE = 50;
    const CLOSE_OK = 51;
    const BLOCKED = 60;
    const UNBLOCKED = 61;
}
