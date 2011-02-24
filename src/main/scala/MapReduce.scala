package it.cnr.aquamaps

import org.apache.hadoop.io._
import com.asimma.ScalaHadoop._
import com.asimma.ScalaHadoop.MapReduceTaskChain._
import com.asimma.ScalaHadoop.ImplicitConversion._
import scala.reflect.Manifest
import scala.collection.JavaConversions._

import com.google.inject._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import net.lag.configgy.{Config, Configgy}

import java.io.StringReader

case class MapReduceModule() extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    bind[EntryPoint].to[MapReduceEntryPoint]
  }
}

case class MapReduceWorkerModule(val row: String) extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    bind[PositionalSource[HCAF]].toInstance(new CSVPositionalSource[HCAF](new StringTableReader(row)))

  }
}

class HCAFMapper
object HCAFMapper extends TypedMapper[LongWritable, Text, Text, LongWritable] {
  //val hspenFile = "file://data/hspen.csv.gz"

  val hspenFile = "hdfs://node1.hadoop.research-infrastructures.eu/user/admin/rainycloud/hspen.csv.gz"
  val hspen = new TableHSPENLoader(new CSVPositionalSource(new HDFSTableReader(hspenFile))).load

  class HadoopEmitter[A <: Keyed](val context: ContextType) extends Emitter[A] {
    def emit(record: A) = context.write(record.key, 1L)
    def flush {}
  }

  override def map(k: LongWritable, v: Text, context: ContextType) {
    val hcafLoader = new TableHCAFLoader(new CSVPositionalSource(new StringTableReader(v)))
    val fetcher = new Fetcher[HCAF] { def fetch(start: String, size: Long) = hcafLoader.load}
    val hspenLoader = new Loader[HSPEN] { def load = hspen }
    val emitter = new HadoopEmitter[HSPEC](context)
    val algorithm = new RandomHSpecAlgorithm()

    val generator = new HSPECGenerator(hspenLoader, emitter, fetcher, algorithm)
    generator.computeInPartition(new Partition(null, 0))
  }
}


class StringTableReader[A](val string: String) extends TableReader[A] {
  def reader = new StringReader(string)
}

class MapReduceEntryPoint extends EntryPoint {

  def run {
    val conf = Configgy.config

    val c = MapReduceTaskChain.init() -->
      //IO.Text[LongWritable, Text]("data/hcaf-small.csv.gz").input -->
      IO.Text[LongWritable, Text](conf.getString("hcafFile").getOrElse("hdfs://node1.hadoop.research-infrastructures.eu/user/admin/rainycloud/hcaf.csv")).input -->
      HCAFMapper -->
      IO.Text[Text, LongWritable]("hdfs://node1.hadoop.research-infrastructures.eu/tmp/outdir").output
    c.execute()
  }

}
