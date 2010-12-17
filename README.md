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
* Cassandra 0.6.x


Run
---

### Workers ###

Run this worker as many times you wish, depending on how many cpus you have. It can also be run on several machines.

    $ octo
    $ ./octobot-jar

The 'octobot' script contains the classpath to the RainyCloud jar. The shipped version points to the jar produced by the sbt build
which lies in `/target/scala_2.8.1/rainycloud_2.8.1-1.0.jar`.

### Job partitioner ###

The worker does nothing untils it gets some messages in the queue.

Currently the jobs are fired by a python script  called `test` under `octo/client`

    $ cd octo/client
    $ ./test <number_of_jobs>
    
If you omit the `number_of_jobs` parameter, the default to process all.

FAQ
---

### Drain the queue ###

When you interrupt the workers the jobs might remain in the queue.
The easiest way to drain the queue (besides using your favourite AMPQ client), is to use this trick:

    $ OCTOBOT_TASKS=. ./octobot
    
This will effectively drain the queue without executing the time consuming tasks, because octobot will not find the code to execute.
