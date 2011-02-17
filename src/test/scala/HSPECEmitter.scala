package it.cnr.aquamaps

import org.specs._

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

import org.specs.mock.Mockito

object HSPECEmitterSpec extends Specification with Mockito {
  "HSPEC emitter" should {
    "emit into csv" in {
      case class TestModule() extends AbstractModule with ScalaModule {
        def configure() {
          bind[TableWriter[HSPEC]].toInstance(new FileSystemTableWriter("/tmp/hspec.csv.gz"))
          bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]]
          bind[Emitter[HSPEC]].to[CSVEmitter[HSPEC]]
        }
      }

      val injector = Guice createInjector TestModule()
      val emitter = injector.instance[Emitter[HSPEC]]
                                      
      emitter.emit(new HSPEC("t1", "c1"));
      emitter.emit(new HSPEC("t2", "c2"));
      emitter.flush
    }
  }
}
