# amqp-async #

<!-- build status -->

**amqp-async** is an implementation of the AMQP 0.9.1 protocol for PHP designed to work with [Icicle](https://github.com/icicleio/icicle).


##### Requirements

- PHP 7.0+

##### Installation


The recommended way to install amqp-async is with the [Composer](http://getcomposer.org/) package manager. (See the [Composer installation guide](https://getcomposer.org/doc/00-intro.md) for information on installing and using Composer.)

Run the following command to use Icicle in your project: 

```bash
composer require DataProcessors/amqp-async
```

You can also manually edit `composer.json` to add amqp-async as a project requirement.

```js
// composer.json
{
    "require": {
        "DataProcessors/amqp-async": "1.0.*"
    }
}
```

#### Example

```php
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
```

#### Credits

**amqp-async** is based on [php-amqplib](https://github.com/videlalvaro/php-amqplib)