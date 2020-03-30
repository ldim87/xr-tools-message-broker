<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 */

namespace XrTools;

use \PhpAmqpLib\Connection\AMQPStreamConnection;
use \PhpAmqpLib\Exception\AMQPProtocolConnectionException;
use \PhpAmqpLib\Channel\AMQPChannel;
use \PhpAmqpLib\Message\AMQPMessage;
use \XrTools\Utils;

/**
 * Message Broker
 */
class MessageBroker
{
	/**
	 * @var array
	 */
	protected $params;

	/**
	 * @var AMQPStreamConnection
	 */
	protected $connect;

	/**
	 * @var AMQPChannel
	 */
	protected $channel;

	/**
	 * @var Utils\DebugMessages
	 */
	protected $dbg;

	/**
	 * MessageBroker constructor.
	 * @param Utils $utils
	 * @param array $connectionParams
	 * @throws \Exception
	 */
	public function __construct(
		Utils $utils,
		array $connectionParams
	){
		$this->dbg = $utils->dbg();

		if (
			empty($connectionParams['host'])
			|| empty($connectionParams['port'])
			|| empty($connectionParams['user'])
			|| empty($connectionParams['password'])
		){
			throw new \Exception('Params list is empty or invalid');
		}

		$this->params = $connectionParams;
	}

	/**
	 * Отправка данных определённому консьюмеру
	 * @param string $consumer
	 * @param string $method
	 * @param mixed  $data
	 * @param array $opt
	 * @return bool
	 * @throws \Exception
	 */
	public function sent(string $consumer, string $method, $data, array $opt = [])
	{
		$debug = ! empty($opt['debug']);

		if (! $this->connect($opt)){
			return false;
		}

		if (empty($consumer)) {
			if ($debug)
				$this->dbg->log('No consumer', __METHOD__);
			return false;
		}

		if (! $msg = $this->prepareMessage($method, $data)){
			return false;
		}

		$this->channel->basic_publish($msg, '', $consumer);

		return true;
	}

	/**
	 * Отправда данных всем консьюмерам
	 * @param string $method
	 * @param mixed  $data
	 * @param array $opt
	 * @return bool
	 * @throws \Exception
	 */
	public function sentAll(string $method, $data, array $opt = [])
	{
		$debug = ! empty($opt['debug']);

		if (! $this->connect($opt)){
			return false;
		}

		if (! $msg = $this->prepareMessage($method, $data)){
			return false;
		}

		$this->channel->basic_publish($msg, 'amq.fanout');

		return true;
	}

	/**
	 * @param string $method
	 * @param mixed  $data
	 * @param array $opt
	 * @return AMQPMessage|bool
	 * @throws \Exception
	 */
	protected function prepareMessage(string $method, $data, array $opt = [])
	{
		$debug = ! empty($opt['debug']);

		if (empty($method)) {
			if ($debug)
				$this->dbg->log('Method not specified', __METHOD__);
			return false;
		}

		$array = [
			'method' => $method,
			'data'   => $data,
		];

		$json = json_encode($array, JSON_UNESCAPED_UNICODE);

		return new AMQPMessage($json);
	}

	/**
	 * Ленивое подключение к RabbitMQ
	 * @param array $opt
	 * @return bool
	 * @throws \Exception
	 */
	protected function connect(array $opt = [])
	{
		$debug = ! empty($opt['debug']);

		if (! empty($this->connect)) {
			return true;
		}

		try {
			$connect = new AMQPStreamConnection(
				$this->params['host'],
				$this->params['port'],
				$this->params['user'],
				$this->params['password']
			);
		}
		catch (AMQPProtocolConnectionException | \ErrorException $e) {
			if ($debug)
				$this->dbg->log( $e->getMessage(), __METHOD__);
			return false;
		}

		$this->connect = $connect;
		$this->channel = $connect->channel();

		return true;
	}
}
