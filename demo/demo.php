<?php
require_once "vendor/autoload.php";

class Demo {

  public function go() {
    $conn = new DataProcessors\AMQP\AMQPConnection();
    yield $conn->connect('127.0.0.1', 5672, 'guest', 'guest');
    $channel = yield $conn->channel();
    yield $channel->basic_consume('test', '', false, false, false, false,
      function($msg) {
        echo "Got a message\n";
      }
    );
  }

}

$demo = new Demo();
$coroutine = new Icicle\Coroutine\Coroutine($demo->go());
Icicle\Loop\run();
