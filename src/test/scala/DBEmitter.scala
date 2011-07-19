package it.cnr.aquamaps
import it.cnr.aquamaps.cloud._

import org.specs._

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

import org.specs.mock.Mockito

import scala.collection.JavaConversions._

object DatabaseHSPECEmitterSpec extends Specification with Mockito {
  "DB HSPEC emitter" should {
    "emit into db" in {
      case class TestModule() extends AbstractModule with ScalaModule {
        def configure() {
          val hspecTable = Table("jdbc:postgresql://dbtest.research-infrastructures.eu/aquamapsorgupdated;username=utente;password=d4science","hspec2011_07_19_16_47_47_005")
          val jobRequest = JobRequest("windows azure", "AQUAPS", null, null, hspecTable, false, true, 1, null, "test", Map[String, String]())
          val taskRequest = TaskRequest(Partition("1000", 21172), jobRequest)

          bind[TaskRequest].toInstance(taskRequest)
          bind[Emitter[HSPEC]].to[DatabaseHSPECEmitter].in[Singleton]          
        }
      }

      val injector = Guice createInjector TestModule()
      val emitter = injector.instance[Emitter[HSPEC]]
                                      
      emitter.emit(new HSPEC("t1", "c1", 0, false, false, 0, 0, 0));
      emitter.emit(new HSPEC("t2", "c2", 0, false, false, 0, 0, 0));
      emitter.flush
    }
  }
}
