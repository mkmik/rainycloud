package it.cnr.aquamaps
import org.apache.log4j.Logger

import me.prettyprint.cassandra.service.{ CassandraHostConfigurator, CassandraClientPoolFactory }
import org.apache.cassandra.thrift.{ Column, SliceRange, SlicePredicate, ColumnParent, ColumnPath, KeyRange, Mutation, ColumnOrSuperColumn, ConsistencyLevel }

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import stopwatch.Stopwatch

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

  val hostConfigurator = new CassandraHostConfigurator("%s:%s".format(cassandraHost, cassandraPort))
  val pool = CassandraClientPoolFactory.getInstance.createNew(hostConfigurator)

  val client = pool borrowClient

  lazy val keyspace = client.getKeyspace(keyspaceName)

  def rangeSlice(from: String, to: String, size: Long, columns: List[String]) = {
    log.info("getting slice %s %s %s  on  %s %s".format(from, to, size, keyspaceName, columnFamily))

    val ks = keyspace
    val range = new KeyRange
    range.setStart_key(from)
    range.setEnd_key("")
    range.setCount(size.asInstanceOf[Int])

    val sp = new SlicePredicate
    val clp = new ColumnParent(columnFamily)

    sp.setColumn_names(columns.map(_.getBytes))

    ks.getRangeSlices(clp, sp, range)
  }

  def batchMutate(mutas : java.util.Map[String, MutationList]) = {
    log.info("upserting %s rows".format(mutas.size))

    Stopwatch("upsert") {
      keyspace.batchMutate(mutas)
    }
  }

}

object CassandraConversions {
  import scala.collection.immutable.HashMap

  implicit def byte2string(x: Array[Byte]): String = new String(x, "utf-8")

  implicit def columnList2map(x: Iterable[Column]): Map[String, Column] = {
    x.foldLeft(Map[String, Column]()) { (acc, v) => acc + (byte2string(v.name) -> v) }
  }

  implicit def columnName(x : Column) : String = byte2string(x.name)
}

trait CassandraFetcher extends Cassandra {
  def columnNames : List[String]

  def fetch(start: String, count: Long) = {
    Stopwatch("fetch") {
      rangeSlice(start, null, count, columnNames)
    }
  }
}

trait CassandraSink extends Cassandra {
  private val log = Logger.getLogger(this.getClass);

  def outputColumnFamily : String
  def batchSize = 10000

  def store(hugeRows: Iterable[Row]) = {
    Stopwatch("store") {
      log.info("storing %s hspec rows".format(hugeRows.size))
      for(rows <- hugeRows.toStream.grouped(batchSize)) {
        def makeRow(row: Row) = row match { case (key, cols) => (key -> makeColumns (cols))}
        def makeMutations(cols: Iterable[Column]) = cols.map {col => mutation(col)}
        def makeColumns(cols : Iterable[Column]) = Map(outputColumnFamily -> makeMutations(cols).toList.asJava).asJava

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
  class NewColumnWrapper(val name: String) {
    def -->(value: String) = newColumn(name, value)
  }

  implicit def newColumnWrapper(name: String) = new NewColumnWrapper(name)

  def newColumn(name: String, value: String) = {
      new Column(name.getBytes, value.getBytes, stamp)
  }

  def stamp = System.currentTimeMillis

}
