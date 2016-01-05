<?php
namespace DataProcessors\AMQP\Constants;

class ExchangeMethods
{
    const DECLARE = 10;
    const DECLARE_OK = 11;
    const DELETE = 20;
    const DELETE_OK = 21;
    const BIND = 30;
    const BIND_OK = 31;
    const UNBIND = 40;
    const UNBIND_OK = 51;
}
