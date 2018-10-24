<?php

namespace June\LaravelKafkaQueue\Queue\Connectors;

use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use June\LaravelKafkaQueue\Queue\KafkaQueue;

class KafkaConnector implements ConnectorInterface
{


    /**
     * @param array $config
     * @return \Illuminate\Contracts\Queue\Queue|void
     */
    public function connect(array $config):Queue
    {
        // TODO: Implement connect() method.
        return new KafkaQueue($config);
    }
}