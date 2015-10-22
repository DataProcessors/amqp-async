<?php

namespace DataProcessors\AMQP\Tests\Unit;

use DataProcessors\AMQP;

class AMQPCollectionTest extends \PHPUnit_Framework_TestCase
{

    public function testSetEmptyKey()
    {
        $t = new AMQP\AMQPTable();

        $this->setExpectedException('DataProcessors\AMQP\Exception\AMQPInvalidArgumentException', 'Table key must be non-empty string up to 128 chars in length');
        $t->set('', 'foo');
        $this->fail('Empty table key not detected!');
    }



    public function testSetLongKey()
    {
        $t = new AMQP\AMQPTable();
        $t->set(str_repeat('a', 128), 'foo');

        $this->setExpectedException('DataProcessors\AMQP\Exception\AMQPInvalidArgumentException', 'Table key must be non-empty string up to 128 chars in length');
        $t->set(str_repeat('a', 129), 'bar');
        $this->fail('Excessive key length not detected!');
    }



    public function testTableRoundTripRabbit()
    {
        $a = new AMQP\AMQPTable($this->getTestDataSrc());
        $this->assertEquals($this->getTestDataCmp(), $a->getNativeData());
    }



    protected function getTestDataSrc()
    {
        return array(
            'long' => 12345,
            'long_neg' => -12345,
            'longlong' => 3000000000,
            'longlong_neg' => -3000000000,
            'float_low' => (float) 9.2233720368548,
            'float_high' => (float) 9223372036854800000,
            'bool_true' => true,
            'bool_false' => false,
            'void' => null,
            'array' => array(1, 2, 3, 'foo', array('bar' => 'baz'), array('boo', false, 5), true, null),
            'array_empty' => array(),
            'table' => array('foo' => 'bar', 'baz' => 'boo', 'bool' => true, 'tbl' => array('bar' => 'baz'), 'arr' => array('boo', false, 5)),
            'table_num' => array(1 => 5, 3 => 'foo', 786 => 674),
            'array_nested' => array(1, array(2, array(3, array(4)))),
            'table_nested' => array('i' => 1, 'n' => array('i' => 2, 'n' => array('i' => 3, 'n' => array('i' => 4))))
        );
    }




    /**
     * The only purpose of this *Cmp / *Cmp080 shit is to pass tests on travis's ancient phpunit 3.7.38
     */
    protected function getTestDataCmp()
    {
        return array(
            'long' => 12345,
            'long_neg' => -12345,
            'longlong' => 3000000000,
            'longlong_neg' => -3000000000,
            'float_low' => (string) (float) 9.2233720368548,
            'float_high' => (string) (float) 9223372036854800000,
            'bool_true' => true,
            'bool_false' => false,
            'void' => null,
            'array' => array(1, 2, 3, 'foo', array('bar' => 'baz'), array('boo', false, 5), true, null),
            'array_empty' => array(),
            'table' => array('foo' => 'bar', 'baz' => 'boo', 'bool' => true, 'tbl' => array('bar' => 'baz'), 'arr' => array('boo', false, 5)),
            'table_num' => array(1 => 5, 3 => 'foo', 786 => 674),
            'array_nested' => array(1, array(2, array(3, array(4)))),
            'table_nested' => array('i' => 1, 'n' => array('i' => 2, 'n' => array('i' => 3, 'n' => array('i' => 4))))
        );
    }



    public function testIterator()
    {
        $d = array('a' => 1, 'b' => -2147, 'c' => array('foo' => 'bar'), 'd' => true, 'e' => false);
        $ed = array(
            'a' => array(AMQP\AMQPAbstractCollection::getDataTypeForSymbol('I'), 1),
            'b' => array(AMQP\AMQPAbstractCollection::getDataTypeForSymbol('I'), -2147),
            'c' => array(AMQP\AMQPAbstractCollection::getDataTypeForSymbol('F'), array('foo' => array(AMQP\AMQPAbstractCollection::getDataTypeForSymbol('S'), 'bar'))),
            'd' => array(AMQP\AMQPAbstractCollection::getDataTypeForSymbol('t'), true),
            'e' => array(AMQP\AMQPAbstractCollection::getDataTypeForSymbol('t'), false)
        );

        $a = new AMQP\AMQPTable($d);

        foreach ($a as $key => $val) {
            if (!isset($d[$key])) {
                $this->fail('Unknown key: ' . $key);
            }
            $this->assertEquals($ed[$key], $val[1] instanceof AMQP\AMQPAbstractCollection ? array($val[0], $this->getEncodedRawData($val[1])) : $val);
        }
    }



    protected function getEncodedRawData(AMQP\AMQPAbstractCollection $c, $recursive = true)
    {
        $r = new \ReflectionProperty($c, 'data');
        $r->setAccessible(true);
        $data = $r->getValue($c);
        unset($r);

        if ($recursive) {
            foreach ($data as &$v) {
                if ($v[1] instanceof AMQP\AMQPAbstractCollection) {
                    $v[1] = $this->getEncodedRawData($v[1]);
                }
            }
            unset($v);
        }

        return $data;
    }
}
