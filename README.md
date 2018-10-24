# laravel-kafka-queue
Laravel queue packages for kafka

**Installation**

1.Install librdkafka and rdkafka


    git clone  https://github.com/edenhill/librdkafka.git  /tmp/librdkafka 
    cd /tmp/librdkafka/ 
    ./configure 
    make && 
    make install
	
	pecl install rdkafka

Then add the following line to your php.ini file:
`extension=rdkafka.so`

2.Use composer to install this package:
`composer require june/laravel-kafka-queue`

3.Add  to providers array :

laravel in `config/app.php`:
`June\LaravelKafkaQueue\LaravelKafkaQueueServiceProvider::class,`

lumen in `boostrap/app.php` :
`$app->register(June\LaravelKafkaQueue\LaravelKafkaQueueServiceProvider::class);`

4.Config :
Change driver to `.env`: `QUEUE_DRIVER=kafka`.
Copy config to `config/queue.php` .

Then you can  use Laravel Queue API.
See:http://laravel.com/docs/queues
