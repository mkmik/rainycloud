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
import au.com.bytecode.opencsv.bean.ColumnPositionMappingStrategy
import java.io._
import java.util.zip._

import CassandraConversions._

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

class DummyHSPENLoader extends HSPENLoader {
  def load = List()
}

trait TableReader[A] {
  def reader: Reader
}

class FileSystemTableReader[A] @Inject() (val name: String) extends TableReader[A] {
  def reader = {
    if (name endsWith ".gz")
      new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(name))))
    else
      new FileReader(name)
  }
}

trait PositionalStore[A] {
  def read: Iterable[Array[String]]
}

class CSVPositionalStore[A] @Inject() (val tableReader: TableReader[A]) extends PositionalStore[A] {
  def read = new CSVReader(tableReader.reader).readAll
}

/** Load some data beans */
trait Loader[A] {
  def load: Iterable[A]
}

/** Used to load the HSPEN table in memory */
trait HSPENLoader extends Loader[HSPEN]

/** Used to load a HCAF table partition in memory */
trait HCAFLoader extends Loader[HCAF]

/** Load the HSPEN table from a positional tabular source (i.e. the colums are known by position). */
class TableHSPENLoader @Inject() (val tableLoader: PositionalStore[HSPEN]) extends HSPENLoader {
  def load = tableLoader.read map HSPEN.fromTableRow
}

/** Load the HCAF table from a positional tabular source (i.e. the colums are known by position). */
class TableHCAFLoader @Inject() (val tableLoader: PositionalStore[HCAF]) extends HCAFLoader {
  def load = tableLoader.read map HCAF.fromTableRow
}

/** Unordered loader which returns named columns */
trait ColumnStoreLoader[A] {
  def read: Iterable[Map[String, String]]
}

/** Provide a column/value output from a csv string. Not very useful, but might be useful for testing */
class CSVColumnStoreLoader[A] @Inject() (val tableReader: TableReader[A]) extends ColumnStoreLoader[A] {

  class ColumnNameMapper(reader: CSVReader) extends ColumnPositionMappingStrategy[A] {
    captureHeader(reader)

    def name(pos: Int) = getColumnName(pos)
    def columns = header

    def asMap(row: Array[String]) = Map(columns zip row: _*)
  }

  def read = {
    val csv = new CSVReader(tableReader.reader)
    val mapper = new ColumnNameMapper(csv)

    csv.readAll map mapper.asMap
  }
}

/** Load the HSPEN table from a column store source */
class ColumnStoreHSPENLoader @Inject() (val columnStoreLoader: ColumnStoreLoader[HSPEN]) extends HSPENLoader {
  def load = columnStoreLoader.read map HSPEN.build
}

/** Loads the HSPEN table from cassandra */
class CassandraLoader[A] @Inject() (val fetcher: CassandraFetcher) extends ColumnStoreLoader[A] {
  val maxSize = 10000

  def read = {
    val hspen = fetcher.fetch("", maxSize)
    hspen.map { case (_, v) => (columnList2map(v) mapValues (x => byte2string(x.value))) }

    // this should work with CassandraConversion._ implicits but it doesn't
    // hspen.map { case (_, v) => columnList2map(v) mapValues (_.value) }
  }
}

/** Generator uses emitter to output data */
trait Emitter[A] {
  def emit(record: A)
}

trait Fetcher[A] {
  def fetch(key: String, size: Long): Iterable[A]
}

/** HSPEC Generator */
trait Generator {
  def computeInPartition(p: Partition)
}

class DummyGenerator @Inject() (val hspenLoader: HSPENLoader) extends Generator {
  def computeInPartition(p: Partition) = println("dummy " + p)
}

class HSPECGenerator @Inject() (val hspenLoader: HSPENLoader, val emitter: Emitter[HSPEC], val fetcher: Fetcher[HCAF], val algorithm: HspecAlgorithm) extends Generator {

  // load HSPEN only once
  lazy val hspen = hspenLoader.load

  def computeInPartition(p: Partition) {
    val records = for {
      // fetch hcaf rows
      hcaf <- fetcher.fetch(p.start, p.size)
      // for each hacf compute a list of hspec
      hspec <- algorithm.compute(hcaf, hspen)
      // yield each hspec converted to a cassandra row
    } emitter.emit(hspec)
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
