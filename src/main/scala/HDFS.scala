package it.cnr.aquamaps

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.weiglewilczek.slf4s.Logging

import java.io.{ InputStream, InputStreamReader, OutputStream, OutputStreamWriter, BufferedReader, BufferedWriter }
import java.util.zip._

import java.net.URI
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

case class HDFSModule() extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    bind[TableReader[HCAF]].toInstance(new HDFSTableReader(conf.getString("hcafFile").getOrElse("data/hcaf.csv.gz")))
    bind[TableReader[HSPEN]].toInstance(new HDFSTableReader(conf.getString("hspenFile").getOrElse("data/hspen.csv.gz")))

    bind[TableWriter[HSPEC]].toInstance(new HDFSTableWriter(conf.getString("hspecFile").getOrElse("/tmp/hspec.csv.gz")))
  }

}


case class COMPSsWorkerHDFSModule() extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    bind[TableReader[HCAF]].toInstance(new HDFSTableReader(conf.getString("hcafFile").getOrElse("data/hcaf.csv.gz")))
    bind[TableReader[HSPEN]].toInstance(new HDFSTableReader(conf.getString("hspenFile").getOrElse("data/hspen.csv.gz")))
  }

}



/*! We can read data from the hdfs filesystem. Gzip files are supported */
class HDFSTableReader[A] @Inject() (val name: String) extends TableReader[A] with HDFSCommon {
  def reader = new BufferedReader(new InputStreamReader(maybeUnzip(fs.open(path))))

  def maybeUnzip(stream: InputStream) = {
    if (name endsWith ".gz")
      new GZIPInputStream(stream)
    else
      stream
  }
}

/*! Same for writer */
class HDFSTableWriter[A] @Inject() (val name: String) extends TableWriter[A] with HDFSCommon with Logging {

  logger.debug("writing to %s".format(name))
  def writer = new BufferedWriter(new OutputStreamWriter(maybeZip(fs.create(path))))

  def maybeZip(stream: OutputStream) = {
    if (name endsWith ".gz")
      new GZIPOutputStream(stream)
    else
      stream
  }
}


/*! Well, I cheated, here are the helpers for the reader and writer */
trait HDFSCommon {
  val conf: Configuration = new Configuration()
  val name: String

  def fs = FileSystem.get(new URI(name), conf)
  def path = new Path(new URI(name).getPath)
}
