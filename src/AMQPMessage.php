<?php
namespace DataProcessors\AMQP;

class AMQPMessage
{
    public $body_size;
    public $tmp_body_parts = array();
    public $body_received = 0;
    public $body;

    /** @var AMQPChannel[] */
    public $delivery_info = array();

    /** @var array Properties content */
    public $properties = array();

    /** @var array */
    protected $prop_types = array(
      'content_type' => 'shortstr',
      'content_encoding' => 'shortstr',
      'application_headers' => 'table_object',
      'delivery_mode' => 'octet',
      'priority' => 'octet',
      'correlation_id' => 'shortstr',
      'reply_to' => 'shortstr',
      'expiration' => 'shortstr',
      'message_id' => 'shortstr',
      'timestamp' => 'timestamp',
      'type' => 'shortstr',
      'user_id' => 'shortstr',
      'app_id' => 'shortstr',
      'cluster_id' => 'shortstr',
    );

    /**
     * @param string $body
     */
    public function __construct(string $body = '', $properties = null)
    {
        $this->body = $body;
        $this->body_size = strlen($body);
        if ($properties) {
            $this->properties = array_intersect_key($properties, $this->prop_types);
        }
    }

    /**
     * Look for additional properties in the 'properties' dictionary,
     * and if present - the 'delivery_info' dictionary.
     *
     * @param string $name
     * @throws \OutOfBoundsException
     * @return mixed|AMQPChannel
     */
    public function get($name)
    {
        if (isset($this->properties[$name])) {
            return $this->properties[$name];
        }

        if (isset($this->delivery_info[$name])) {
            return $this->delivery_info[$name];
        }

        throw new \OutOfBoundsException(sprintf(
            'No "%s" property',
            $name
        ));
    }

    /**
     * Sets a property value
     *
     * @param string $name The property name (one of the property definition)
     * @param mixed $value The property value
     * @throws \OutOfBoundsException
     */
    public function set($name, $value)
    {
        if (!array_key_exists($name, $this->prop_types)) {
            throw new \OutOfBoundsException(sprintf(
                'No "%s" property',
                $name
            ));
        }

        $this->properties[$name] = $value;
    }

    public function load_properties(AMQPBufferReader $r)
    {
        // Read 16-bit shorts until we get one with a low bit set to zero
        $flags = array();
        while (true) {
            $flag_bits = $r->read_short();
            $flags[] = $flag_bits;
            if (($flag_bits & 1) == 0) {
                break;
            }
        }

        $shift = 0;
        $d = array();
        foreach ($this->prop_types as $key => $proptype) {
            if ($shift == 0) {
                if (!$flags) {
                    break;
                }
                $flag_bits = array_shift($flags);
                $shift = 15;
            }
            if ($flag_bits & (1 << $shift)) {
                $d[$key] = $r->{'read_' . $proptype}();
            }

            $shift -= 1;
        }
        $this->properties = $d;
    }

    /**
     * Serializes the 'properties' attribute (a dictionary) into the
     * raw bytes making up a set of property flags and a property
     * list, suitable for putting into a content frame header.
     *
     * @return string
     * @todo Inject the AMQPWriter to make the method easier to test
     */
    public function serialize_properties()
    {
        $shift = 15;
        $flag_bits = 0;
        $flags = array();
        $raw_bytes = new AMQPBufferWriter();

        foreach ($this->prop_types as $key => $prototype) {
            $val = isset($this->properties[$key]) ? $this->properties[$key] : null;

            // Very important: PHP type eval is weak, use the === to test the
            // value content. Zero or false value should not be removed
            if ($val === null) {
                $shift -= 1;
                continue;
            }

            if ($shift === 0) {
                $flags[] = $flag_bits;
                $flag_bits = 0;
                $shift = 15;
            }

            $flag_bits |= (1 << $shift);
            if ($prototype != 'bit') {
                $raw_bytes->{'write_' . $prototype}($val);
            }

            $shift -= 1;
        }

        $flags[] = $flag_bits;
        $result = new AMQPBufferWriter();
        foreach ($flags as $flag_bits) {
            $result->write_short($flag_bits);
        }

        $result->write($raw_bytes->getvalue());

        return $result->getvalue();
    }
}
