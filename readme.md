# AMQP for Icicle #

[![Build Status](https://img.shields.io/travis/DataProcessors/amqp-async.svg?style=flat-square)](https://travis-ci.org/DataProcessors/amqp-async)
[![LGPL-2.1 License](https://img.shields.io/packagist/l/DataProcessors/amqp-async.svg?style=flat-square)](LICENSE)

This library is a component for [Icicle](https://github.com/icicleio/icicle) that provides an AMQP 0.9.1 client implementation. Like other Icicle components, this library uses [Promises](https://github.com/icicleio/icicle/wiki/Promises) and [Generators](http://www.php.net/manual/en/language.generators.overview.php) for asynchronous operations that may be used to build [Coroutines](https://github.com/icicleio/icicle/wiki/Coroutines) to make writing asynchronous code more like writing synchronous code.

##### Requirements

- PHP 7.0+

##### Installation

The recommended way to install is with the [Composer](http://getcomposer.org/) package manager. (See the [Composer installation guide](https://getcomposer.org/doc/00-intro.md) for information on installing and using Composer.)

Run the following command to use the library in your project: 

```bash
composer require icicleio/amqp
```

You can also manually edit `composer.json` to add this library as a project requirement.

```js
// composer.json
{
    "require": {
        "icicleio/amqp": "^0.1"
    }
}
```

#### Example

```php
<?php
require_once "vendor/autoload.php";

class Demo {

  public function go() {
    $conn = new Icicle\AMQP\AMQPConnection();
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