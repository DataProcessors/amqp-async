<?php
namespace DataProcessors\AMQP\Constants;

class TxMethods
{
    const SELECT = 10;
    const SELECT_OK = 11;
    const COMMIT = 20;
    const COMMIT_OK = 21;
    const ROLLBACK = 30;
    const ROLLBACK_OK = 31;
}
