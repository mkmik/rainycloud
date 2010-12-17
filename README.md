Rainy Cloud
===========

Rainy Cloud is a worker node for Acquamaps on the cloud.

Currently it implements only the implementation of the distributed HSPEC calculation.

Build
-----

In order to build the project you need Simply-Build-Tool (SBT):

http://code.google.com/p/simple-build-tool/

Here is a quick guide to install the sbt launcher.

    $ cd ~/bin
    $ wget http://simple-build-tool.googlecode.com/files/sbt-launch-0.7.5.RC0.jar
    $ echo 'java -Xmx512M -jar `dirname $0`/sbt-launch-0.7.5.RC0.jar "$@"' >sbt
    $ chmod u+x sbt
    
(I'm using 0.7.5.RC0, I don't know if it works with the sbt-launcher 0.7.4, probably it will)

Then in order to build it, simply run:

    $ sbt update   # updates jar dependencies 
    $ sbt package  # builds and creates the jar

Run time dependencies
---------------------

* RabbitMQ (`apt-get install rabbitmq-server` or equivalent)
* python ampq client (`apt-get install python-amqplib` or equivalent)
* Cassandra 0.6.x

Configure
---------

### RabbitMQ ###

I've tested it with rabbitmq but it should work with any AMQP and other queue protocols supported by Octobot (see Octobot doc).
The file has to live in the current directory when running `octobot-jar`

`octo/config.yaml` contains the AMQP broker hostname, port, credentials and queue name, please adapt to your configuration

It also contains the number of workers to be spawned in a single octobot instances, you might consider increasing that number if you want
to exploit SMP environments.

Create rabbitmq user and permissions:

    # rabbitmqctl add_user foo bar
    # rabbitmqctl set_permissions foo '.*' '.*' '.*'a
    
You can change username/pw in the `octo/config.yaml`


### Cassandra ###

Download cassandra, for example from `http://it.apache.contactlab.it//cassandra/0.6.8/apache-cassandra-0.6.8-bin.tar.gz`

Copy the bundled storage-conf.xml from `/cassandra/storage-conf.xml` to the `conf` dir of the cassandra distribution, and start cassandra.

`octo/config.yaml` contains the cassandra hostname/ip which will be contacted by the worker. It defaults to `localhost`, so if you intend to run that
on a real cluster please change the hostname.

In order to avoid that writing to cassandra becomes a bottleneck, the software should be tested on a cassandra cluster.

This worker reads data from cassandra, but initially your cassandra will be empty, so you have to put some data there.

There is a mysql dump in `/data/aqua.dump.bz2`, which has to be imported on a mysql server somewhere. Then you can fire `scripts/importer.py`.
Please edit the `improter.py` script in order to change the mysql connection parameters and the target cassandra ip/hostname and port.

Run
---

### Workers ###

Run this worker as many times you wish (load permitting). It can also be run on several machines.

    $ octo
    $ ./octobot-jar

The 'octobot' script contains the classpath to the RainyCloud jar. The shipped version points to the jar produced by the sbt build
which lies in `/target/scala_2.8.1/rainycloud_2.8.1-1.0.jar`.

### Job partitioner ###

The worker does nothing untils it gets some messages in the queue.

Currently the jobs are fired by a python script  called `test` under `octo/client`

    $ cd octo/client
    $ ./test <number_of_chunks>
    
If you omit the `number_of_chunks` parameter, the default to process all.

The partitioner is not very sofisticated, all it does is to read a `ranges` file and send messages to the queue with the 
key prefixes and sizes parameters read from that file. The keys are static for this application scenario as they map the 
locations on earth surface which hopefully won't change until humankind explores new planets, and starts to pollute them :-)

FAQ
---

### Drain the queue ###

When you interrupt the workers the jobs might remain in the queue.
The easiest way to drain the queue (besides using your favourite AMQP client), is to use this trick:

    $ OCTOBOT_TASKS=. ./octobot
    
This will effectively drain the queue without executing the time consuming tasks, because octobot will not find the code to execute.
