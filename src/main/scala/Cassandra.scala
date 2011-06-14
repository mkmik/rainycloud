package it.cnr.aquamaps
import org.apache.log4j.Logger

import me.prettyprint.cassandra.service.{ CassandraHostConfigurator, ThriftCluster }
import me.prettyprint.hector.api.factory.HFactory
import org.apache.cassandra.thrift.{ Column, SliceRange, SlicePredicate, ColumnParent, ColumnPath, KeyRange, Mutation, ColumnOrSuperColumn, ConsistencyLevel }
import me.prettyprint.cassandra.serializers.StringSerializer
import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import stopwatch.Stopwatch

/*!## Parked

 This code works but has to be refactored to follow the new design of `Loader`/`Fetcher`/`Emitter` */

trait CassandraConfig {
  def keyspaceName: String
  def columnFamily: String

  type Row = (String, Iterable[Column])
  type MutationList = java.util.Map[String, java.util.List[Mutation]]
}

trait CassandraConnectionConfig extends CassandraConfig {
  def cassandraHost: String
  def cassandraPort: Int
}

trait Cassandra extends CassandraConnectionConfig {
  private val log = Logger.getLogger(this.getClass);

  import CassandraConversions._

  val hostConfigurator = new CassandraHostConfigurator("%s:%s".format(cassandraHost, cassandraPort))
  val cluster = new ThriftCluster("Research Infrastructures", hostConfigurator)

  lazy val keyspace = HFactory.createKeyspace(keyspaceName, cluster)

  def rangeSlice(from: String, to: String, size: Long, columns: List[String]) = {
    log.info("getting slice %s %s %s  on  %s %s".format(from, to, size, keyspaceName, columnFamily))

    val serializer = StringSerializer.get
    val rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, serializer, serializer, serializer)
    rangeSlicesQuery.setColumnFamily(columnFamily)
    rangeSlicesQuery.setKeys(from, "")
    rangeSlicesQuery.setRowCount(size.asInstanceOf[Int])

    val result = rangeSlicesQuery.execute()

  }

  def batchMutate(mutas: java.util.Map[String, MutationList]) = {
    val serializer = StringSerializer.get
    log.info("upserting %s rows".format(mutas.size))

    Stopwatch("upsert") {
      val mutator = HFactory.createMutator(keyspace, serializer)
      //...
    }
  }

}

object CassandraConversions {
  import scala.collection.immutable.HashMap

  implicit def string2bytebuffer(x: String): ByteBuffer = ByteBuffer.wrap(x.getBytes)
  implicit def bytes2bytebuffer(x: Array[Byte]): ByteBuffer = ByteBuffer.wrap(x)

  implicit def bytebuffer2string(x: ByteBuffer): String = new String(x.array, "utf-8")
  implicit def byte2string(x: Array[Byte]): String = new String(x, "utf-8")

  implicit def columnList2map(x: Iterable[Column]): Map[String, Column] = {
    x.foldLeft(Map[String, Column]()) { (acc, v) => acc + (byte2string(v.name.array) -> v) }
  }

  implicit def columnName(x: Column): String = byte2string(x.name.array)
}

trait CassandraFetcher extends Cassandra {
  def columnNames: List[String]

  def fetch(start: String, count: Long) = {
    Stopwatch("fetch") {
      rangeSlice(start, null, count, columnNames)
    }
  }
}

trait CassandraSink extends Cassandra {
  private val log = Logger.getLogger(this.getClass);

  def outputColumnFamily: String
  def batchSize = 10000

  def store(hugeRows: Iterable[Row]) = {
    Stopwatch("store") {
      log.info("storing %s hspec rows".format(hugeRows.size))
      for (rows <- hugeRows.toStream.grouped(batchSize)) {
        def makeRow(row: Row) = row match { case (key, cols) => (key -> makeColumns(cols)) }
        def makeMutations(cols: Iterable[Column]) = cols.map { col => mutation(col) }
        def makeColumns(cols: Iterable[Column]) = Map(outputColumnFamily -> makeMutations(cols).toList.asJava).asJava

        val mutations = rows.foldLeft(Map[String, MutationList]()) { (acc, row) => acc + makeRow(row) }
        batchMutate(mutations.asJava)
      }
    }
  }

  final def mutation(column: Column) = {
    val mut = new Mutation
    val color = new ColumnOrSuperColumn
    mut.setColumn_or_supercolumn(color)
    color.setColumn(column)
    mut
  }

  def simpleMutation(key: String, column: Column) = (key, mutation(column))

}

trait CassandraCreator extends CassandraConfig {
  import CassandraConversions._

  class NewColumnWrapper(val name: String) {
    def -->(value: String) = newColumn(name, value)
  }

  implicit def newColumnWrapper(name: String) = new NewColumnWrapper(name)

  def newColumn(name: String, value: String) = {
    new Column(name, value, stamp)
  }

  def stamp = System.currentTimeMillis

}
