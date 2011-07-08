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
    $ wget http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-tools.sbt/sbt-launch/0.10.0/sbt-launch.jar
    $ echo 'java -Xmx512M -jar `dirname $0`/sbt-launch.jar "$@"' >sbt
    $ chmod u+x sbt
   

Then in order to build it, simply run:

    $ sbt update   # updates jar dependencies 
    $ sbt assembly # builds and creates the jar and self-contained uberjar

Run
---

You can run it quickly locally:

    sbt run

Or you can run the self-contained jar:

    java -jar ./target/scala_2.8.1/rainycloud_2.8.1-assembly-1.0.jar

Config
------

There is a configuration file in `rainycloud.conf`, it contains paths to the partitions, list of modules to be activated etc.

Run jar with `--help` to get info about command line switches.

For example you can enable the `COMPSs` module manually either by adding `COMPSs` do the modules in the config file or via commandline:

    java -jar ./target/scala_2.8.1/rainycloud_2.8.1-assembly-1.0.jar -m COMPSs


Doc
---

You can find docs generated with docco [here](http://mmikulicic.github.com/rainycloud/docco/)

FAQ
---

* It takes too long, how can I reduce the input data so that I can test it faster ?

The data is in data/hcaf.csv.gz and data/hspen.csv.gz. You can consider to create a hcaf.csv.gz files containing only a few hundred lines. But if you do it manually you should also
update the partition map file in octo/client/ranges.

So you can use the provided data/hcaf-small.csv.gz (copy it over the hcaf.csv.gz file)

The other way is to reduce the number of partitions:

    java -jar ./target/scala_2.8.1/rainycloud_2.8.1-assembly-1.0.jar -r octo/client/rangesSmall

* how can I run one instance of the "worker", computing only one partition

    java -jar ./target/scala_2.8.1/rainycloud_2.8.1-assembly-1.0.jar  -e "100 1000" --hcaf data/hcaf.csv.gz --hspen data/hspen.csv.gz --hspec /tmp/out.gz
