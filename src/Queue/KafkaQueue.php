<?php

namespace June\LaravelKafkaQueue\Queue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Facades\Log;
use June\LaravelKafkaQueue\Queue\Jobs\KafkaJob;
use RuntimeException;


class KafkaQueue extends Queue implements QueueContract
{

    protected $config;
    protected $sleepOnError;

    public function __construct(Array $config)
    {
        $this->config       = $config;
        $this->sleepOnError = $config['sleep_on_error'] ?? 5;
    }


    /**
     * Get the size of the queue.
     *
     * @param  string $queue
     * @return int
     */
    public function size($queue = null)
    {
        return 1;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string|object $job
     * @param  mixed $data
     * @param  string $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string $payload
     * @param  string $queue
     * @param  array $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {

        try {
            $rk = new \RdKafka\Producer();
            $rk->setLogLevel(LOG_DEBUG);
            $rk->addBrokers($this->config['brokers']);

            $queueName = $this->getTopic($queue);

            $topic = $rk->newTopic($queueName);
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $this->getKey());

        } catch (\Exception $e) {
            $this->reportConnectionError('pushRaw', $e);
        }

    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTimeInterface|\DateInterval|int $delay
     * @param  string|object $job
     * @param  mixed $data
     * @param  string $queue
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        $rk = new \RdKafka\Consumer();
        $rk->setLogLevel(LOG_DEBUG);
        $rk->addBrokers($this->config['brokers']);
        $topicConf = new \RdKafka\TopicConf();
        $topicConf->set('auto.commit.enable', 'false');

        $queueName = $this->getTopic($queue);
        $topic     = $rk->newTopic($queueName, $topicConf);
        $topic->consumeStart($this->config['partition'], RD_KAFKA_OFFSET_STORED);

        $message = $topic->consume($this->config['partition'], 1000);

        switch($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                //save the offset for next time
               $topic->offsetStore($message->partition, $message->offset);

                return new KafkaJob($this->container, $this, $message,
                    $this->connectionName, $this->getTopic($queue));
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
        }

    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param  string $job
     * @param  mixed $data
     * @param  string $queue
     *
     * @return array
     */
    protected function createPayloadArray($job, $data = '', $queue = null)
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'attempts' => 0,
        ]);
    }

    /** get the key
     * @param string $key
     * @return string
     */
    public function getKey($key = '')
    {
        return $key ?: uniqid('kf', true);
    }

    /**get the kafka topic name
     * @param $queue
     * @return mixed
     */
    protected function getTopic($queue)
    {
        return $queue ?: $this->config['queue'];
    }

    /**
     * @param $action
     * @param \Exception $e
     */
    protected function reportConnectionError($action, \Exception $e)
    {
        Log::error('Kafka error while attempting '.$action.': '.$e->getMessage());

        if($this->sleepOnError === false) {
            throw new RuntimeException('Error writing data to the connection with Kafka', null, $e);
        }

        sleep($this->sleepOnError);
    }
}