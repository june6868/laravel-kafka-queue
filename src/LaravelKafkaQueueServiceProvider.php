<?php

namespace June\LaravelKafkaQueue;

use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use June\LaravelKafkaQueue\Queue\Connectors\KafkaConnector;

class LaravelKafkaQueueServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap the application services.
     *
     * @return void
     */
    public function boot()
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        $connector = new KafkaConnector();
        $queue->addConnector('kafka', function () use ($connector) {
            return $connector;
        });
    }

    /**
     * Register the application services.
     *
     * @return void
     */
    public function register()
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/kafka.php', 'queue.connections.kafka'
        );
    }
}
