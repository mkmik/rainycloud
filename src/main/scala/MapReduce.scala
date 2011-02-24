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

  override def map(k: LongWritable, v: Text, context: ContextType) {

    val source = new CSVPositionalSource[HCAF](new StringTableReader(v))
    val loader = new TableHCAFLoader(source)
    val records = loader.load

    val algorithm = new RandomHSpecAlgorithm()

    for {
      hcaf <- records
      hspec <- algorithm.compute(hcaf, hspen)
    } context.write(hspec.key, 1L)

  }
}

class StringTableReader[A](val string: String) extends TableReader[A] {
  def reader = new StringReader(string)
}

class MapReduceEntryPoint extends EntryPoint {

  def run {
    val c = MapReduceTaskChain.init() -->
      //IO.Text[LongWritable, Text]("data/hcaf-small.csv.gz").input -->
      IO.Text[LongWritable, Text]("hdfs://node1.hadoop.research-infrastructures.eu/user/admin/rainycloud/hcaf-small.csv.gz").input -->
      HCAFMapper -->
      IO.Text[Text, LongWritable]("hdfs://node1.hadoop.research-infrastructures.eu/tmp/outdir").output
    c.execute()
  }

}
