package it.cnr.aquamaps

import org.specs._

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

object CSVLoaderSpec extends Specification {
  case class TestModule() extends AbstractModule with ScalaModule {
    def configure() {
      bind[HSPENLoader].to[TableHSPENLoader]
      bind[HCAFLoader].to[TableHCAFLoader]

      bind[TableReader[HSPEN]].toInstance(new FileSystemTableReader("data/hspen.csv.gz"))
      bind[TableReader[HCAF]].toInstance(new FileSystemTableReader("data/hcaf.csv.gz"))
      bind[PositionalStore[HSPEN]].to[CSVPositionalStore[HSPEN]]
      bind[PositionalStore[HCAF]].to[CSVPositionalStore[HCAF]]
    }
  }

  "CSV loader" should {
    val injector = Guice createInjector TestModule()

    "load hspen" in {
      val loader = injector.instance[HSPENLoader]
      loader.load.size must be_>(100)
    }

    "load hcaf" in {
      val loader = injector.instance[HCAFLoader]
      loader.load.size must be_>(100)
    }

  }
}
