package it.cnr.aquamaps

import org.apache.log4j.Logger

import me.prettyprint.cassandra.service.{ CassandraHostConfigurator, CassandraClientPoolFactory }
import org.apache.cassandra.thrift.{ Column, SliceRange, SlicePredicate, ColumnParent, ColumnPath, KeyRange }

import scala.collection.JavaConversions._

trait Cassandra {
  private val log = Logger.getLogger(this.getClass);

  val hostConfigurator = new CassandraHostConfigurator("localhost:9160")
  val pool = CassandraClientPoolFactory.getInstance.createNew(hostConfigurator)

  val client = pool borrowClient

  def keyspaceName: String
  def keyspace = client.getKeyspace(keyspaceName)

	def columnFamily: String

  def rangeSlice(from: String, to: String, size: Long, columns: List[String]) = {

    log.info("getting slice %s %s %s  on   %s %s".format(from, to, size, keyspaceName, columnFamily))

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
}

object CassandraConversions {
  import scala.collection.immutable.HashMap

  implicit def byte2string(x: Array[Byte]): String = new String(x, "utf-8")

  implicit def columnList2map(x: Iterable[Column]): Map[String, Column] = {
    x.foldLeft(new HashMap[String, Column]) { (acc, v) => acc + (byte2string(v.name) -> v) }
  }

	implicit def columnName(x : Column) : String = byte2string(x.name)
}

trait CassandraFetcher extends Cassandra {
  def fetch(start: String, count: Long) = {
    rangeSlice(start, null, count, HCAF.columns)
  }
}

trait CassandraSink extends Cassandra {
  private val log = Logger.getLogger(this.getClass);

  def store(rows: Iterable[Column]) = {
    log.info("storing")
  }
}
