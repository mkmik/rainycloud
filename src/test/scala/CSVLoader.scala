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
      bind[Loader[HSPEN]].to[TableHSPENLoader]
      bind[Loader[HCAF]].to[TableHCAFLoader]

      bind[TableReader[HSPEN]].toInstance(new FileSystemTableReader("data/hspen.csv.gz"))
      bind[TableReader[HCAF]].toInstance(new FileSystemTableReader("data/hcaf.csv.gz"))
      bind[PositionalSource[HSPEN]].to[CSVPositionalSource[HSPEN]]
      bind[PositionalSource[HCAF]].to[CSVPositionalSource[HCAF]]
    }
  }

  "CSV loader" should {
    val injector = Guice createInjector TestModule()

    "load hspen" in {
      val loader = injector.instance[Loader[HSPEN]]
      loader.load.size must be_>(100)
    }

    "load hcaf" in {
      val loader = injector.instance[Loader[HCAF]]
      loader.load.size must be_>(100)
    }

  }
}
