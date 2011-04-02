Quick Run:

    rainycloud.bat

Output file in:

    hspec.csv.gz



Partitions are described in the 'huge-ranges.txt' file. Each line defines a partition. A single partition can be passed as parameter:

    java -jar rainycloud_2.8.1-assembly-1.0.jar  -e "100 1000"

or you can put one or more partitions in a file and run them sequentially:

    java -jar rainycloud_2.8.1-assembly-1.0.jar  -r huge-ranges.txt
  

The output file defaults to hspec.csv.gz, but can be overridden with:

    java -jar rainycloud_2.8.1-assembly-1.0.jar  -e "100 1000" --hspec part001.gz

File names are treated as file:// urls (so use always / as dir separator). Adding ".gz" to the filename will compress it.
The quotes around the partition descriptor are important.

The result of each run of a worker shall be then concatenated in the resulting file.
Real world scenario will use a table storage output etc, this is only meant for testing and demonstration purposes.


The first run loads data under the 'data/' directory and creates a temporary directory "babudb" in the current working directory.
This "babudb" speeds up subsequent invocations of the worker.

(Tested using wine and jre 1.6)
