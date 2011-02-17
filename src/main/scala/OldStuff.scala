package it.cnr.aquamaps

import com.urbanairship.octobot.Settings

import com.google.inject._
import com.google.inject.name._

import org.apache.log4j.Logger
import stopwatch.Stopwatch

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.cassandra.thrift.{ Column }


trait TaskConfig {
  def taskName: String
}

trait CassandraTaskConfig extends TaskConfig with CassandraConnectionConfig {
  override def cassandraHost = Settings.get(taskName, "cassandra_host")
  override def cassandraPort = Settings.get(taskName, "cassandra_port").toInt
  override def keyspaceName = Settings.get(taskName, "cassandra_keyspace")
}

trait HSPECGeneratorTaskConfig extends TaskConfig {
  def taskName = "HSPECGenerator"
}


/*! Loads the HSPEN table from cassandra */
class CassandraLoader[A] @Inject() (val fetcher: CassandraFetcher) extends ColumnStoreLoader[A] {
  val maxSize = 10000

  def read = {
    import CassandraConversions._
    // this should work with CassandraConversion._ implicits but it doesn't
    // hspen.map { case (_, v) => columnList2map(v) mapValues (_.value) }

    val hspen = fetcher.fetch("", maxSize)
    hspen.map { case (_, v) => (columnList2map(v) mapValues (x => byte2string(x.value))) }
  }
}


class OldHSPECGenerator @Inject() (hspenLoader: HSPENLoader) extends Generator with CassandraTaskConfig with HSPECGeneratorTaskConfig with CassandraFetcher with CassandraSink with Watch {
  private val log = Logger.getLogger(this.getClass);

  override def columnFamily = "hcaf"
  override def outputColumnFamily = "hspec"
  override def columnNames = HCAF.columns

  // actual logic is here
  val algorithm = new SimpleHSpecAlgorithm

  // load HSPEN only once
  lazy val hspen = hspenLoader.load

  // worker, accepts slices of HCAF table and computes HSPEC
  def computeInPartition(p: Partition) {
    val start = p.start
    val size = p.size

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
