package it.cnr.aquamaps

import org.apache.log4j.Logger
import org.json.simple.JSONObject
import org.json.simple.JSONArray

import org.apache.cassandra.thrift.{ Column }

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import io.Source.fromFile

import CassandraConversions._

import stopwatch.Stopwatch

import com.urbanairship.octobot.Settings

import com.google.inject._
import com.google.inject.name._

import au.com.bytecode.opencsv.CSVReader
import java.io._
import java.util.zip._

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

/** Used to load the HSPEN table in memory */
trait HSPENLoader {
  def load : Iterable[HSPEN]
}

class DummyHSPENLoader extends HSPENLoader {
  def load = List()
}

trait TableReader {
   def reader(name: String) : Reader
}

class FileSystemTableReader extends TableReader {
   def reader(name: String) = {
    if(name endsWith ".gz")
      new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(name))))
    else
      new FileReader(name)
  }
}

trait TableLoader {  
  def read(name : String) : Iterable[Array[String]]
}

class CSVTableLoader @Inject() (
  val tableReader : TableReader
)
extends TableLoader {
  def read(name : String) = new CSVReader(tableReader.reader(name)).readAll
}

/** Load the HSPEN table from a file in the filesystem */
class TableHSPENLoader @Inject() (
  @Hspen
  val fileName : String = "data/hspen.csv.gz",
  val tableLoader : TableLoader
) extends HSPENLoader {
 
  // def load = (tableLoader.read(fileName) map HSPEN.fromTableRow).toIterable
  def load = tableLoader.read(fileName) map HSPEN.fromTableRow
}


object CassandraHSPENLoader extends HSPENLoader with CassandraFetcher with CassandraTaskConfig with HSPECGeneratorTaskConfig {
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

/** HSPEC Generator */
trait Generator {

  @Inject
  var hspenLoader : HSPENLoader = _

  def computeInPartition(p: Partition)
}

class DummyGenerator extends Generator {
  def computeInPartition(p: Partition) = println("dummy " + p)
}


class HSPECGenerator extends Generator with CassandraTaskConfig with HSPECGeneratorTaskConfig with CassandraFetcher with CassandraSink with Watch {
  private val log = Logger.getLogger(this.getClass);

  override def columnFamily = "hcaf"
  override def outputColumnFamily = "hspec"
  override def columnNames = HCAF.columns

  // actual logic is here
  val algorithm = new SimpleHSpecAlgorithm

  // load HSPEN only once
  lazy val hspen = CassandraHSPENLoader.load

  // worker, accepts slices of HCAF table and computes HSPEC
  def computeInPartition(p:  Partition) {
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
