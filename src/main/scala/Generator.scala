package it.cnr.aquamaps

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import io.Source.fromFile

import stopwatch.Stopwatch

import com.google.inject._
import com.google.inject.name._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import au.com.bytecode.opencsv.bean.ColumnPositionMappingStrategy
import java.io._
import java.util.zip._


/*!## Table readers

 A table reader declares a data source for a PositionalStore (or others). We exploit phantom types to easily bind
 a particular implementation of a table reader to a given PositionalStore via Guice.
 */
trait TableReader[A] {
  def reader: Reader
}

/*! We can read data from the filesystem. Gzip files are supported */
class FileSystemTableReader[A] @Inject() (val name: String) extends TableReader[A] {
  def reader = {
    if (name endsWith ".gz")
      new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(name))))
    else
      new FileReader(name)
  }
}

/*! Loads data from a positional store, like a table with ordered columns but without column names */
trait PositionalStore[A] {
  def read: Iterable[Array[String]]
}

/*! We treat CSV as a positional store, we rely on the fact that the columns are in a particular order */
class CSVPositionalStore[A] @Inject() (val tableReader: TableReader[A]) extends PositionalStore[A] {
  def read = new CSVReader(tableReader.reader).readAll
}

/*!## Loaders */
 
/*! A Loader loads a whole db in as an iterable. */
trait Loader[A] {
  def load: Iterable[A]
}

/*! Used to load the HSPEN table in memory */
trait HSPENLoader extends Loader[HSPEN]

/*! Used to load a HCAF table partition in memory */
trait HCAFLoader extends Loader[HCAF]

/*! Load the HSPEN table from a positional tabular source (i.e. the colums are known by position). */
class TableHSPENLoader @Inject() (val tableLoader: PositionalStore[HSPEN]) extends HSPENLoader {
  def load = tableLoader.read map HSPEN.fromTableRow
}

/*! Load the HCAF table from a positional tabular source (i.e. the colums are known by position). */
class TableHCAFLoader @Inject() (val tableLoader: PositionalStore[HCAF]) extends HCAFLoader {
  def load = tableLoader.read map HCAF.fromTableRow
}

/*! A ColumnStoreLoader provides the data differently. It returns data as unordered name-value pairs. Some data sources, like
 table stores can returns data like this. */
trait ColumnStoreLoader[A] {
  def read: Iterable[Map[String, String]]
}

/*! We can provide a column/value output from a csv string. Not very useful, but might be useful for testing */
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

/*! Load the HSPEN table from a column store source */
class ColumnStoreHSPENLoader @Inject() (val columnStoreLoader: ColumnStoreLoader[HSPEN]) extends HSPENLoader {
  def load = columnStoreLoader.read map HSPEN.build
}

/*!## Emitter
 
 Generator uses Emitter to output data. Usually the Emitter emits HSPEC records.
 */
trait Emitter[A] {
  def emit(record: A)

  def flush
}


/*! If a table is a Product (a case class is a product) then we can serialize it to csv via this emitter. */
class CSVEmitter[A <: Product] extends Emitter[A] {
  val writer = new CSVWriter(new FileWriter(new File("/tmp/test.csv")))

  def emit(record: A) = writer.writeNext(record.productIterator.map(_.toString).toArray)

  def flush = writer.flush
}

/*!## Fetcher
 
 A Fetcher is a component that reads some data from a db. It's mostly useful for loading partitions of the HCAF table.

 A Fetcher differs from a Loader, in that it supports the fetch by range functionality, and it's implementation may be unable
 to load the whole content in one piece.
 
 Please see the MemoryFetcher.scala file for a concrete fetcher
 */
trait Fetcher[A] {
  def fetch(key: String, size: Long): Iterable[A]
}


/*!## Generator */

/*! This is the heart of the scenario. The entry point invokes this method once for each partition.  */
trait Generator {
  def computeInPartition(p: Partition)
}


/*! This is a local implementation of the HSPEC generator core. */
class HSPECGenerator @Inject() (val hspenLoader: HSPENLoader, val emitter: Emitter[HSPEC], val fetcher: Fetcher[HCAF], val algorithm: HspecAlgorithm) extends Generator {

  /*! HSPEN table is loaded only once (hence lazy)*/
  lazy val hspen = hspenLoader.load

  /*! Then for each partition: */
  def computeInPartition(p: Partition) {
    val records = for {
      /*! * fetch hcaf rows for that partition */
      hcaf <- fetcher.fetch(p.start, p.size)
      /*! * for each hacf row compute a list of output hspec rows*/
      hspec <- algorithm.compute(hcaf, hspen)
      /*! * emit each generated hspec row using our pluggable emitter */
    } emitter.emit(hspec)
  }

}



