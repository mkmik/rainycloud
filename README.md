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

* RabbitMQ
* Cassandra 0.6.x


Run
---

Run this worker as many times you wish, depending on how many cpus you have. It can also be run on several machines.

    $ octo
    $ ./octobot-jar

The 'octobot' script contains the classpath to the RainyCloud jar. The shipped version points to the jar produced by the sbt build
which lies in `/target/scala_2.8.1/rainycloud_2.8.1-1.0.jar`.


