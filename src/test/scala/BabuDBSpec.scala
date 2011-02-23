package it.cnr.aquamaps

import org.specs._

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

import org.specs.mock.Mockito
import org.mockito.Matchers._ // to use matchers like anyInt()

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object BabuDBSpec extends Specification with Mockito {
  case class TestModule() extends AbstractModule with ScalaModule {
    def configure() {
      bind[Loader[HCAF]].to[TableHCAFLoader]
      bind[TableReader[HCAF]].toInstance(new FileSystemTableReader("data/hcaf.csv.gz"))
      bind[PositionalSource[HCAF]].to[CSVPositionalSource[HCAF]]
      bind[Serializer[HCAF]].to[SerializableSerializer[HCAF]]

    }

    @Provides
    @Singleton
    def hcafFetcher(loader: Loader[HCAF], serializer: Serializer[HCAF]): Fetcher[HCAF] = new BabuDBFetcher("hcaf", loader, serializer)

  }

  "BabuDB initializer" should {
    val serializer = new SerializableSerializer[String] {}

    "serialize objects" in {
      val res = serializer.serialize("test")
      println(res)
    }
  }

  "BabuDB fetcher" should {
    val injector = Guice createInjector TestModule()

    val fetcher = injector.instance[Fetcher[HCAF]]

    "first page" in {
      val rows = fetcher.fetch("1000", 231)

      rows.size must be_==(231)
      rows.head.key must be_==("1000:100:1")
      rows.last.key must be_==("1000:455:4")
    }

    "second page" in {
      val rows = fetcher.fetch("1004", 111)

      rows.size must be_==(111)
      rows.head.key must be_==("1004:102:2")
      rows.last.key must be_==("1004:489:2")

    }
    fetcher.shutdown

  }
}
