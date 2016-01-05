<?php
namespace DataProcessors\AMQP\Constants;

class QueueMethods
{
    const DECLARE = 10;
    const DECLARE_OK = 11;
    const BIND = 20;
    const BIND_OK = 21;
    const PURGE = 30;
    const PURGE_OK = 31;
    const DELETE = 40;
    const DELETE_OK = 41;
    const UNBIND = 50;
    const UNBIND_OK = 51;
}
