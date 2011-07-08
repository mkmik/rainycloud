package it.cnr.aquamaps

import org.specs._

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

import org.specs.mock.Mockito
import org.mockito.Matchers._  // to use matchers like anyInt()

import scala.io.Source.fromFile

object GeneratorSpec extends Specification with Mockito {
  case class TestModule() extends AbstractModule with ScalaModule with RainyCloudModule {
    def configure() {
      bind[Loader[HSPEN]].to[TableHSPENLoader]

      bind[TableReader[HSPEN]].toInstance(new FileSystemTableReader("data/hspen.csv.gz"))
      bind[PositionalSource[HSPEN]].to[CSVPositionalSource[HSPEN]]

      bind[Partitioner].toInstance(new StaticPartitioner(ranges))

      bind[Generator].to[HSPECGenerator]

      bind[HspecAlgorithm].to[AllHSpecAlgorithm]

      bind[Emitter[HSPEC]].toInstance(mock[Emitter[HSPEC]])
      bind[Fetcher[HCAF]].toInstance(mock[Fetcher[HCAF]])
    }

    def ranges: Iterator[String] = {
      val ranges = conf.getList("inlineranges")
      if(ranges.isEmpty)
        fromFile(conf.getString("ranges").getOrElse("octo/client/ranges")).getLines
      else
        ranges.toIterator
    }

  }

  "HSPEC generator" should {
    val injector = Guice createInjector TestModule()

    "compute in partition" in {
      val partitioner = injector.instance[Partitioner]
      val generator = injector.instance[Generator]

      val fetcher = injector.instance[Fetcher[HCAF]]
      setup(fetcher)
      val emitter = injector.instance[Emitter[HSPEC]]
      
      val p = partitioner.partitions.next
      generator.computeInPartition(p)

      val hspenLoader = injector.instance[Loader[HSPEN]]
      val size = 2 * hspenLoader.load.size

      there was size.times(emitter).emit(anyObject())
    }

  }

  def setup(fetcher: Fetcher[HCAF]) {
    val h1 = HCAF.makeHcaf
    h1.csquareCode = "1000:100:1"

    val h2 = HCAF.makeHcaf
    h2.csquareCode = "1000:100:2"

    fetcher.fetch("1000", 231) returns List(h1, h2)
  }
}
