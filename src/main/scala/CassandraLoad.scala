package it.cnr.aquamaps

import me.prettyprint.cassandra.service.{ CassandraHostConfigurator, ThriftCluster }
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.serializers._
import java.io.FileInputStream
import java.nio.ByteBuffer

object CassandraLoad extends App {

  val cassandraHost = "node1.cassandra"
  val cassandraPort = 9160
  val keyspaceName = "dnet"
  val columnFamily = "body"

  val hostConfigurator = new CassandraHostConfigurator("%s:%s,node2.cassandra:9160,node3.cassandra:9160".format(cassandraHost, cassandraPort))
  hostConfigurator.setAutoDiscoverHosts(true)
  val cluster = new ThriftCluster("Research Infrastructures", hostConfigurator)

  def randomPayload: ByteBuffer = {
    val file = new FileInputStream("/dev/urandom")
    try {
      val ch = file.getChannel
      val buffer = ByteBuffer.allocate(2 * 1024)
      ch.read(buffer)
      buffer.rewind
      return buffer
    } finally {
      file.close
    }
  }

  try {
    val stringSerializer = StringSerializer.get
    val byteSerializer = ByteBufferSerializer.get
    val keyspace = HFactory.createKeyspace(keyspaceName, cluster)
    //keyspace.setConsistencyLevel(

    val records = 3400000

    for (i <- 1 to records / 64) {
      val mutator = HFactory.createMutator(keyspace, stringSerializer)
      for (j <- 1 to 64) {
        mutator.addInsertion("user " + i + ", " + j, columnFamily, HFactory.createColumn("name " + i, randomPayload, stringSerializer, byteSerializer))
      }
      mutator.execute()
      println("saved batch " + i + " record " + (i * 64))
    }

    println("Wow")

  } finally {
    cluster.getConnectionManager().shutdown()
  }
}
