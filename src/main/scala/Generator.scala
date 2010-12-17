package it.cnr.aquamaps

import org.apache.log4j.Logger
import org.json.simple.JSONObject
import org.json.simple.JSONArray

import org.apache.cassandra.thrift.{ Column }

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import CassandraConversions._

import stopwatch.Stopwatch

import com.urbanairship.octobot.Settings

trait TaskConfig {
  def taskName : String
}

trait CassandraTaskConfig extends TaskConfig with CassandraConnectionConfig {
  override def cassandraHost = Settings.get(taskName, "cassandra_host")
  override def cassandraPort = Settings.get(taskName, "cassandra_port").toInt
  override def keyspaceName = Settings.get(taskName, "cassandra_keyspace")
}

trait HSPECGeneratorTaskConfig extends TaskConfig {
  def taskName = "HSPECGenerator"
}

object HSPENLoader extends CassandraFetcher with CassandraTaskConfig with HSPECGeneratorTaskConfig {
  private val log = Logger.getLogger(this.getClass);

  override def columnFamily = "hspen"
  override def columnNames = HSPEN.columns

  // load HSPEN table
  def load = {
    val hspen = fetch("", 10000)
    log.info("hspen loaded: " + hspen.size)

    hspen.map { case (_, v) => HSPEN.fromCassandra(v) }
  }
}

object HSPECGenerator extends Worker with CassandraTaskConfig with HSPECGeneratorTaskConfig with CassandraFetcher with CassandraSink with Watch {
  private val log = Logger.getLogger(this.getClass);

  override def columnFamily = "hcaf"
  override def outputColumnFamily = "hspec"
  override def columnNames = HCAF.columns

  // actual logic is here
  val algorithm = new SimpleHSpecAlgorithm

  // load HSPEN only once
  lazy val hspen = HSPENLoader.load

  // worker, accepts slices of HCAF table and computes HSPEC
  def run(task: JSONObject) {
    val start = task.get("start").asInstanceOf[String]
    val size = task.get("size").asInstanceOf[Long]

    val records = for {
      // fetch hcaf rows
      (_, hcaf) <- fetch(start, size)
      // for each hacf compute a list of hspec
      hspec <- compute(hcaf)
      // yield each hspec converted to a cassandra row
    } yield hspec.toCassandra 

    store(records)
  }

  // create a domain object from table store columns and compute
  def compute(hcafColumns: Iterable[Column]): Iterable[HSPEC] = {

    val hcaf = HCAF.fromCassandra(hcafColumns)

    Stopwatch("compute") {
      algorithm.compute(hcaf, hspen);
    }
  }

}
