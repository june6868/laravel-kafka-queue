<?php

namespace June\LaravelKafkaQueue\Queue\Jobs;

use Exception;
use Illuminate\Support\Str;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\Jobs\JobName;
use Illuminate\Container\Container;
use Illuminate\Database\DetectsDeadlocks;
use Illuminate\Contracts\Queue\Job as JobContract;
use June\LaravelKafkaQueue\Queue\KafkaQueue;

class KafkaJob extends Job implements JobContract
{
    use DetectsDeadlocks;

    protected $kafka;
    protected $queue;
    protected $message;


    public function __construct(Container $container, KafkaQueue $kafka, $message, $connectionName, $queue)
    {
        $this->container      = $container;
        $this->message        = $message;
        $this->kafka          = $kafka;
        $this->connectionName = $connectionName;
        $this->queue          = $queue;

    }

    /**
     * Fire the job.
     *
     * @throws Exception
     *
     * @return void
     */
    public function fire()
    {
        try {
            $payload = $this->payload();

            list($class, $method) = JobName::parse($payload['job']);

            with($this->instance = $this->resolve($class))->{$method}($this, $payload['data']);
        } catch (Exception $exception) {
            if (
                $this->causedByDeadlock($exception) ||
                Str::contains($exception->getMessage(), ['detected deadlock'])
            ) {
                sleep(2);
                $this->fire();

                return;
            }

            throw $exception;
        }
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->message->offset ?? null;
    }

    /**
     * Get the raw body of the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->message->payload;
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return ($this->payload()['attempts'] ?? null) + 1;
    }


}