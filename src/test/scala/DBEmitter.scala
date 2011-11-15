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
          //val hspecTable = Table("jdbc:postgresql://dbtest.research-infrastructures.eu/aquamapsorgupdated;username=utente;password=d4science", "hspec_test_loading")

          val hspecTable = Table("jdbc:postgresql://localhost:5433/aquamapsorgupdated;username=utente;password=d4science", "hspec_test_loading")
          val jobRequest = JobRequest("windows azure", "AQUAMAPS", null, null, hspecTable, false, true, 1, null, "test", Map[String, String]())
          val taskRequest = TaskRequest(Partition("1000", 21172), jobRequest)

          bind[TaskRequest].toInstance(taskRequest)
          bind[Emitter[HSPEC]].to[CopyDatabaseHSPECEmitter].in[Singleton]

          bind[CSVSerializer].to[CompatCSVSerializer]
        }
      }

      val injector = Guice createInjector TestModule()
      val emitter = injector.instance[Emitter[HSPEC]]

      //for(i <- 1 to 152647576) {
      for(i <- 1 to 100) {
        emitter.emit(new HSPEC("t%s".format(i), "c%s".format(i), 0, false, false, 2, 0, ""));
      }
      emitter.flush
    }
  }
}
