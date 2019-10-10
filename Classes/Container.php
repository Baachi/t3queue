<?php

declare(strict_types=1);

namespace DMK\T3Queue;

use DMK\T3Queue\Exception\InvalidTransportException;
use DMK\T3Queue\Exception\MissingConfigurationException;
use Enqueue\AmqpBunny\AmqpConnectionFactory as AmqpBunnyConnectionFactory;
use Enqueue\AmqpExt\AmqpConnectionFactory;
use Enqueue\AmqpLib\AmqpConnectionFactory as AmqpLibConnectionFactory;
use Enqueue\Dbal\DbalConnectionFactory;
use Enqueue\Fs\FsConnectionFactory;
use Enqueue\Gearman\GearmanConnectionFactory;
use Enqueue\Gps\GpsConnectionFactory;
use Enqueue\Mongodb\MongodbConnectionFactory;
use Enqueue\Null\NullConnectionFactory;
use Enqueue\Pheanstalk\PheanstalkConnectionFactory;
use Enqueue\RdKafka\RdKafkaConnectionFactory;
use Enqueue\Redis\RedisConnectionFactory;
use Enqueue\Sns\SnsConnectionFactory;
use Enqueue\SnsQs\SnsQsConnectionFactory;
use Enqueue\Sqs\SqsConnectionFactory;
use Enqueue\Stomp\StompConnectionFactory;
use Enqueue\Wamp\WampConnectionFactory;
use Interop\Queue\ConnectionFactory;
use Interop\Queue\Context;
use Psr\Container\ContainerInterface;
use TYPO3\CMS\Core\Configuration\ExtensionConfiguration;
use TYPO3\CMS\Core\SingletonInterface;

final class Container implements ContainerInterface, SingletonInterface
{
    /**
     * @var ExtensionConfiguration
     */
    private $configuration;

    private $transports = [
        'amqp' => AmqpConnectionFactory::class,
        'amqp_bunny' => AmqpBunnyConnectionFactory::class,
        'amqp_lib' => AmqpLibConnectionFactory::class,
        'dbal' => DbalConnectionFactory::class,
        'fs' => FsConnectionFactory::class,
        'gearman' => GearmanConnectionFactory::class,
        'gps' => GpsConnectionFactory::class,
        'kafka' => RdKafkaConnectionFactory::class,
        'mongodb' => MongodbConnectionFactory::class,
        'null' => NullConnectionFactory::class,
        'pheanstalk' => PheanstalkConnectionFactory::class,
        'redis' => RedisConnectionFactory::class,
        'sns' => SnsConnectionFactory::class,
        'sns_sqs' => SnsQsConnectionFactory::class,
        'sqs' => SqsConnectionFactory::class,
        'stomp' => StompConnectionFactory::class,
        'wamp' => WampConnectionFactory::class
    ];

    public function __construct(ExtensionConfiguration $configuration)
    {
        $this->configuration = $configuration;
    }

    public function get($id)
    {
        return $this->resolve($id);
    }

    public function has($id)
    {
        try {
            $this->resolve($id);
            return true;
        } catch (MissingConfigurationException $e) {
            return false;
        }
    }


    /**
     * Creates a context.
     *
     * @param string $name
     *
     * @return Context
     *
     * @throws InvalidTransportException
     * @throws MissingConfigurationException
     * @throws \TYPO3\CMS\Core\Configuration\Exception\ExtensionConfigurationExtensionNotConfiguredException
     * @throws \TYPO3\CMS\Core\Configuration\Exception\ExtensionConfigurationPathDoesNotExistException
     */
    private function resolve(string $name): Context
    {
        $configuration = $this->configuration->get('t3queue', 'queue');

        if (empty($configuration[$name])) {
            throw new MissingConfigurationException(sprintf(
                'There are no configuration for "%s".',
                $name
            ));
        }

        $params = $configuration[$name];
        $driver = $params['transport'] ?? null;

        unset($params['transport']);

        if (!isset($this->transports[$driver])) {
            throw new InvalidTransportException(sprintf(
                'The transport "%s" does not exists. Valid values are %s',
                $driver,
                implode(', ', array_keys($this->transports))
            ));
        }


        /** @var ConnectionFactory $factory */
        $class = $this->transports[$driver];
        $factory = new $class($params);

        return $factory->createContext();
    }
}
