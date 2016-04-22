<?php
namespace DataProcessors\AMQP;

use Icicle\Coroutine\Coroutine;
use Icicle\Socket\Connector\DefaultConnector;
use Icicle\Awaitable\Deferred;
use Icicle\Awaitable;
use DataProcessors\AMQP\Constants\Constants091;
use DataProcessors\AMQP\Constants\ClassTypes;
use DataProcessors\AMQP\Constants\FrameTypes;
use DataProcessors\AMQP\Constants\ConnectionMethods;
use DataProcessors\AMQP\Constants\ChannelMethods;
use DataProcessors\AMQP\Constants\BasicMethods;

class AMQPConnection
{
    /** @var array */
    public static $LIBRARY_PROPERTIES = array(
        'product' => array('S', 'DP_AMQP'),
        'platform' => array('S', 'PHP'),
        'version' => array('S', '1.0'),
        'information' => array('S', ''),
        'copyright' => array('S', ''),
        'capabilities' => array(
            'F',
            array(
                'authentication_failure_close' => array('t', true),
                'consumer_cancel_notify' => array('t', true)
            )
        )
    );

    /** @var AMQPChannel[] */
    protected $channels = array();

    /** @var Client */
    protected $client = null;

    /** @var AMQPBufferReader */
    protected $bufferReader;

    /** @var Protocol091 */
    protected $protocolWriter;

    /** @var int */
    protected $heartbeat = null;

    /** @var bool */
    protected $something_received_between_heartbeat_checks = false;

    /** @var bool */
    protected $something_sent_between_heartbeat_checks = false;

    /** @var int */
    protected $channel_max = 65535;

    /** @var int */
    protected $frame_max = 131072;

    /** @var int */
    public $version_major;

    /** @var int */
    public $version_minor;

    /** @var array */
    public $server_properties;

    /** @var string */
    public $mechanisms;

    /** @var string */
    public $locales;

    public function __construct()
    {
        $this->bufferReader = new AMQPBufferReader();
        $this->protocolWriter = new Protocol091();
        $this->channels[0] = new AMQPChannel($this, 0); // Create default control channel 0
    }

    /**
    * Dump human readable frame information for debugging
    *
    * @param int $frame_type
    * @param int $channel_id
    * @param string $payload
    */
    protected function frameInfo(int $frame_type, int $channel_id, string $payload)
    {
        if ($frame_type == FrameTypes::METHOD) {
            if (strlen($payload) >= 4) {
                $method_sig_array = unpack('n2', substr($payload, 0, 4));
                $method_sig = $method_sig_array[1] . ',' . $method_sig_array[2];

                return Constants091::$FRAME_TYPES[$frame_type] . " " .
                    sprintf(
                        '> %s: %s',
                        $method_sig,
                        Constants091::$GLOBAL_METHOD_NAMES[MiscHelper::methodSig($method_sig)]
                    );
            }
        } else {
            return Constants091::$FRAME_TYPES[$frame_type];
        }
    }

    /**
     * Requests a connection close
     *
     * @param int $reply_code
     * @param string $reply_text
     * @param int $failing_class_id
     * @param int $failing_method_id
     * @return mixed|null
     */
    public function close(int $reply_code = 0, string $reply_text = '', int $failing_class_id = 0, int $failing_method_id = 0)
    {
        if (!$this->isOpen()) {
            yield null;
            return;
        }

        yield $this->closeChannels();

        yield $this->sendChannelMethodFrame(0, $this->protocolWriter->connectionClose($reply_code, $reply_text, $failing_class_id, $failing_method_id));

        $deferred = new Deferred();
        $this->channels[0]->add_wait([['class_id'=>ClassTypes::CONNECTION, 'method_id'=>ConnectionMethods::CLOSE_OK]], $deferred, null, null);
        yield $deferred->getPromise();
        yield $this->client->close();
    }

    /**
     * Closes all available channels
     */
    protected function closeChannels()
    {
        foreach ($this->channels as $key => $channel) {
            // don't close channel 0
            if ($key === 0) {
                continue;
            }
            try {
                yield $channel->close();
            } catch (\Exception $e) {
                /* Ignore closing errors */
            }
        }
    }

    /**
    * Return true if connection is open
    *
    */
    public function isOpen()
    {
        return ($this->client !== null) && ($this->client->isOpen());
    }

    /**
    * Provide access to underlying socket stream resource
    */
    public function getResource()
    {
        return $this->client->getResource();
    }

    /**
    * Connect to AMQP server
    *
    * @param string $host
    * @param string $port
    * @param string $user
    * @param string $password
    * @param string $vhost
    * @param string $login_method
    * @param string $locale
    * @param int $heartbeat
    */
    public function connect(string $host, string $port, string $user, string $password, string $vhost = '/', string $login_method = 'AMQPLAIN', string $locale = 'en_US', int $heartbeat = null)
    {
        $this->heartbeat = $heartbeat;
        $connector = new DefaultConnector();
        $this->client = (yield $connector->connect($host, $port));
        yield $this->client->write(Constants091::AMQP_PROTOCOL_HEADER);
        $payload = (yield $this->syncWaitChannel0([['class_id'=>ClassTypes::CONNECTION, 'method_id'=>ConnectionMethods::START]]));
        $this->connectionStart($payload);
        yield $this->startOk(self::$LIBRARY_PROPERTIES, $login_method, $this->getLoginResponse($user, $password), $locale);
        $payload = (yield $this->syncWaitChannel0([['class_id'=>ClassTypes::CONNECTION, 'method_id'=>ConnectionMethods::TUNE]]));
        $this->connectionTune($payload);
        yield $this->tuneOk($this->channel_max, $this->frame_max, $this->heartbeat);
        yield $this->open($vhost);
        yield $this->syncWaitChannel0([['class_id'=>ClassTypes::CONNECTION, 'method_id'=>ConnectionMethods::OPEN_OK]]);
        ///
        $coroutine = new Coroutine($this->pump()); // pump needs to stay running as a coroutine, reading and dispatching messages
        $coroutine->done();
        ///
        if ($this->heartbeat > 0) {
            (new Coroutine($this->outputHeartbeatLoop()))->done();
            (new Coroutine($this->inputHeartbeatLoop()))->done();
        }
    }

    /**
    * Generate login_response value for start_ok
    *
    * @param string $user
    * @param string $password
    * @return string
    */
    protected function getLoginResponse(string $user, string $password)
    {
        if ($user && $password) {
            $login_response = new AMQPBufferWriter();
            $login_response->write_table(array(
                'LOGIN' => array('S', $user),
                'PASSWORD' => array('S', $password)
            ));
            // Skip the length
            $responseValue = $login_response->getvalue();
            $login_response = substr($responseValue, 4, strlen($responseValue) - 4);
        } else {
            $login_response = null;
        }
        return $login_response;
    }

    /**
    * Wait for methods(s) on channel 0
    *
    * @param array $methods
    */
    protected function syncWaitChannel0(array $methods)
    {
        $ch = $this->channels[0];
        list($frame_type, $channel_id, $payload) = (yield $this->waitForFrame());
        if ($channel_id !== 0) {
            throw new Exception\AMQPRuntimeException("Expecting frame on channel 0");
        }
        $ch->add_wait($methods, null, null, null);

        if (!$ch->isWaitFrame($frame_type, $payload)) {
            throw new Exception\AMQPRuntimeException("Waiting for " . implode(",", $methods) . " but received [" . $this->frameInfo($frame_type, $channel_id, $payload) . "]");
        }

        yield $payload;
    }

    /**
    * Start connection negotiation
    *
    * @param string $payload
    */
    protected function connectionStart(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $this->bufferReader->reuse($args);
        $this->version_major = $this->bufferReader->read_octet();
        $this->version_minor = $this->bufferReader->read_octet();
        $this->server_properties = $this->bufferReader->read_table();
        $this->mechanisms = $this->bufferReader->read_longstr();
        $this->locales = $this->bufferReader->read_longstr();
    }

    /**
    * Proposes connection tuning parameters
    *
    * @param string $payload
    */
    protected function connectionTune(string $payload)
    {
        $args = substr($payload, 4, strlen($payload) - 4);
        $this->bufferReader->reuse($args);
        $v = $this->bufferReader->read_short();
        if ($v) {
            $this->channel_max = $v;
        }

        $v =  $this->bufferReader->read_long();
        if ($v) {
            $this->frame_max = $v;
        }

        // use server proposed value if not set
        if ($this->heartbeat === null) {
            $this->heartbeat = $this->bufferReader->read_short();
        }
    }

    /**
     * Negotiates connection tuning parameters
     *
     * @param int $channel_max
     * @param int $frame_max
     * @param int $heartbeat
     */
    protected function tuneOk(int $channel_max, int $frame_max, int $heartbeat)
    {
        yield $this->sendChannelMethodFrame(0, $this->protocolWriter->connectionTuneOk($channel_max, $frame_max, ($heartbeat === null ? 0 : $heartbeat)));
    }


    /**
    * Waits for a frame from the server, skipping heartbeat frames.
    *
    * @return array
    */
    public function waitForFrame()
    {
        while (true) {
            list($frame_type, $channel, $payload) = (yield $this->waitForAnyFrame());
            $this->something_received_between_heartbeat_checks = true;
            if (!($channel === 0 && $frame_type === FrameTypes::HEARTBEAT)) { // If not heartbeat frame then we are done
                break;
            }
        }
    }

    /**
    * Reads until specified number of bytes has been read.
    *
    * @param int $bytesToRead
    */
    protected function readExactly(int $bytesToRead)
    {
        $data = '';
        while ($bytesToRead > 0) {
            $chunk = (yield $this->client->read($bytesToRead));
            $bytesToRead -= strlen($chunk);
            $data .= $chunk;
        }
        yield $data;
    }

    /**
    * Waits for a frame from the server.
    *
    * @return array
    * @throws \DataProcessors\AMQP\Exception\AMQPRuntimeException
    */
    protected function waitForAnyFrame()
    {
        // frame_type + channel_id + size
        $data = (yield $this->readExactly(AMQPBufferReader::OCTET + AMQPBufferReader::SHORT + AMQPBufferReader::LONG));
        $this->bufferReader->reuse($data);

        $frame_type = $this->bufferReader->read_octet();
        $channel = $this->bufferReader->read_short();
        $size = $this->bufferReader->read_long();

        // payload + ch
        $data = (yield $this->readExactly(AMQPBufferReader::OCTET + (int) $size));

        $this->bufferReader->reuse($data);

        $payload = $this->bufferReader->read($size);
        $ch = $this->bufferReader->read_octet();

        if ($ch != Constants091::FRAME_END) {
            throw new Exception\AMQPRuntimeException(sprintf(
                'Framing error, unexpected byte: %x',
                $ch
            ));
        }

        yield [$frame_type, $channel, $payload];
    }

    /**
     * @param array $client_properties
     * @param string $mechanism
     * @param string $response
     * @param string $locale
     */
    protected function startOk(array $client_properties, string $mechanism, string $response, string $locale)
    {
        yield $this->sendChannelMethodFrame(0, $this->protocolWriter->connectionStartOk($client_properties, $mechanism, $response, $locale));
    }

    /**
     * Sends a method frame
     *
     * @param int $channel_id
     * @param array $frame
     */
    public function sendChannelMethodFrame(int $channel_id, array $frame)
    {
        $pkt = new AMQPBufferWriter();
        $pkt->write_octet(1);
        $pkt->write_short($channel_id);
        $pkt->write_long(strlen($frame[2]) + 4); // payload-size = length of args + length of class_id + length of method_id
        // in payload
        $pkt->write_short($frame[0]); // class_id
        $pkt->write_short($frame[1]); // method_id
        $pkt->write($frame[2]); // args

        $pkt->write_octet(Constants091::FRAME_END);

        $this->something_sent_between_heartbeat_checks = true;
        yield $this->client->write($pkt->getvalue());
    }

    /**
     * Sends content (HEADER/BODY frames)
     *
     * @param int $channel_id
     * @param int $class_id
     * @param int $weight
     * @param int $body_size
     * @param string $packed_properties
     * @param string $body
     */
    public function sendChannelContent(int $channel_id, int $class_id, int $weight, int $body_size, string $packed_properties, string $body)
    {
        $w = new AMQPBufferWriter();

        /// HEADER ///
        $w->write_octet(2);
        $w->write_short($channel_id);
        $w->write_long(strlen($packed_properties) + 12);
        $w->write_short($class_id);
        $w->write_short($weight);
        $w->write_longlong($body_size);
        $w->write($packed_properties);
        $w->write_octet(Constants091::FRAME_END);

        /// BODY ///
        $position = 0;
        while ($position < $body_size) {
            $payload = substr($body, $position, $this->frame_max - 8);
            $position += $this->frame_max - 8;

            $w->write_octet(3);
            $w->write_short($channel_id);
            $w->write_long(strlen($payload));

            $w->write($payload);

            $w->write_octet(Constants091::FRAME_END);
        }

        $this->something_sent_between_heartbeat_checks = true;
        yield $this->client->write($w->getvalue());
    }

    /**
    * @param string $vhost
    * @param string $reserved1
    * @param bool $reserved2
    */
    protected function open(string $vhost, string $reserved1 = '', bool $reserved2 = false)
    {
        yield $this->sendChannelMethodFrame(0, $this->protocolWriter->connectionOpen($vhost, $reserved1, $reserved2));
    }

    /**
     * Fetches a channel object identified by the numeric channel_id, or
     * create that object if it doesn't already exist.
     *
     * @param int $channel_id
     * @return AMQPChannel
     */
    public function channel(int $channel_id = null)
    {
        // garbage collect channels
        foreach ($this->channels as $i => $channel) {
            if ($channel->isClosed()) {
                unset($this->channels[$i]);
            }
        }
        //
        if (isset($this->channels[$channel_id])) {
            yield $this->channels[$channel_id];
        } else {
            $channel_id = $channel_id ? $channel_id : $this->getFreeChannelID();
            $ch = new AMQPChannel($this, $channel_id);
            $this->channels[$channel_id] = $ch;
            yield $ch->open();
            yield $ch;
        }
    }

    /**
     * @return int
     * @throws \DataProcessors\AMQP\Exception\AMQPRuntimeException
     */
    protected function getFreeChannelID()
    {
        for ($i = 1; $i <= $this->channel_max; $i++) {
            if (!isset($this->channels[$i])) {
                return $i;
            }
        }
        throw new Exception\AMQPRuntimeException('No free channel ids');
    }

    /**
     * Processing loop
     */
    public function pump()
    {
        while ($this->isOpen()) {
            yield $this->next();
        }
    }

    /**
     * Output heartbeat loop
     */
    public function outputHeartbeatLoop()
    {
        while ($this->isOpen()) {
            yield Awaitable\resolve()->delay($this->heartbeat);
            if (!$this->something_sent_between_heartbeat_checks) {
                $pkt = new AMQPBufferWriter();
                $pkt->write_octet(FrameTypes::HEARTBEAT);
                $pkt->write_short(0);
                $pkt->write_long(0);
                $pkt->write_octet(Constants091::FRAME_END);
                yield $this->client->write($pkt->getvalue());
            } else {
                $this->something_sent_between_heartbeat_checks = false;
            }
        }
    }

    /**
     * Input heartbeat loop
     */
    public function inputHeartbeatLoop()
    {
        while ($this->isOpen()) {
            yield Awaitable\resolve()->delay($this->heartbeat * 2);
            if (!$this->something_received_between_heartbeat_checks) {
                yield $this->close();
            } else {
                $this->something_received_between_heartbeat_checks = false;
            }
        }
    }

    /**
     * Process next frame
     */
    public function next()
    {
        list($frame_type, $channel_id, $payload) = (yield $this->waitForFrame());

        //echo "[" . date('Y-m-d H:i:s') . "] Received frame on channel $channel_id [" . $this->frameInfo($frame_type, $channel_id, $payload) . "] payload size " . strlen($payload) . "\n";

        if (!isset($this->channels[$channel_id])) {
            throw new Exception\AMQPRuntimeException("Received frame on non-existent channel number $channel_id");
        } else {
            $ch = $this->channels[$channel_id];
        }

        if (strlen($payload) < 4) {
            throw new Exception\AMQPOutOfBoundsException('Method frame too short');
        }

        list(, $class_id, $method_id) = unpack('n2', substr($payload, 0, 4));

        if (($frame_type == FrameTypes::METHOD) && ($class_id == ClassTypes::BASIC) && ($method_id == BasicMethods::DELIVER)) {
            if (!$ch->pendingClose) {
                $ch->basicDeliver($payload);
            }
        } elseif ($frame_type == FrameTypes::HEADER) {
            if (!$ch->pendingClose) {
                $ch->frameHeader($payload);
            }
        } elseif ($frame_type == FrameTypes::BODY) {
            if (!$ch->pendingClose) {
                $ch->frameBody($payload);
            }
        } elseif (($frame_type == FrameTypes::METHOD) && ($class_id == ClassTypes::BASIC) && ($method_id == BasicMethods::CANCEL)) {
            // RabbitMQ specific extension which sends a Basic.cancel to the client in some situations; enabled with consumer_cancel_notify in connect options
            // see https://www.rabbitmq.com/consumer-cancel.html
        } elseif (($frame_type == FrameTypes::METHOD) && ($class_id == ClassTypes::BASIC) && ($method_id == BasicMethods::RETURN)) {
            if (!$ch->pendingClose) {
                $ch->basicReturn($payload);
            }
        } elseif (($frame_type == FrameTypes::METHOD) && ($class_id == ClassTypes::CHANNEL) && ($method_id == ChannelMethods::FLOW)) {
            if (!$ch->pendingClose) {
                yield $ch->serverFlow($payload);
            }
        } else {
            if ($ch->isWaitFrame($frame_type, $payload)) {
                // got the frame we were looking for
            } else {
                throw new Exception\AMQPRuntimeException("Unexpected frame received [" . $this->frameInfo($frame_type, $channel_id, $payload) . "]");
            }
        }
    }
}
