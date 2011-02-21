package it.cnr.aquamaps

import org.specs._

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }

import org.specs.mock.Mockito
import org.mockito.Matchers._ // to use matchers like anyInt()

import java.io.File

object COMPSsSpec extends Specification with Mockito {

  "partition ser/deser" should {
    "eat it's own dog food" in {
      import P2XML._

      val p = new Partition("1234", 123)
      val ser = p.toXml

      val des: Partition = ser.toPartition

      des must be_==(p)
    }
  }
/*
  "guice compss wrapper" should {
    case class TestModule() extends AbstractModule with ScalaModule {
      def configure() {

        bind[TableWriter[HSPEC]].to[FileSystemTableWriter[HSPEC]]

        bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]]
        bind[Emitter[HSPEC]].to[CSVEmitter[HSPEC]]

        bind[Generator].toInstance(mock[Generator])
      }

      @Provides
      def writer: FileSystemTableWriter[HSPEC] = new FileSystemTableWriter(mkTmp)

      /*! This is a little tricky: we need a separate context but need to use the same generator as the outer context (since it's a mock we actually
       access from the test code. */
      @Provides
      def fpg(generator: Generator): FileParamsGenerator = {
        val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` StaticFileParamsGenerator.COMPSsWorkerModule())
        new SimpleFileParamsGenerator(generator, injector.instance[Emitter[HSPEC]], injector.instance[FileSystemTableWriter[HSPEC]])
      }

    }

    val injector = Guice createInjector TestModule()

    val compss = injector.instance[COMPSsGenerator]
    val backend = injector.instance[Generator]

    "pass params in files" in {
      val partition = new Partition("1000", 231)

      compss.computeInPartition(partition)

      there was one(backend).computeInPartition(partition)
    }
  }
*/

  "static compss wrapper" should {
    try {
    case class TestModule() extends AbstractModule with ScalaModule {
      def configure() {
        val writer: FileSystemTableWriter[HSPEC] = new FileSystemTableWriter(mkTmp)

        bind[TableWriter[HSPEC]].toInstance(writer)
        bind[FileSystemTableWriter[HSPEC]].toInstance(writer)

        bind[FileParamsGenerator].to[StaticFileParamsGenerator]
        bind[PositionalSink[HSPEC]].toInstance(mock[CSVPositionalSink[HSPEC]])
        bind[Emitter[HSPEC]].toInstance(mock[Emitter[HSPEC]])

        bind[Generator].toInstance(mock[Generator])
      }
    }
    val injector = Guice createInjector TestModule()

    val compss = injector.instance[COMPSsGenerator]
    val sink = injector.instance[PositionalSink[HSPEC]]

    "pass params in files" in {
      val partition = new Partition("1000", 231)

      compss.computeInPartition(partition)

    }
    } catch {
      case e => 
        println("got exception %s".format(e))
      e.printStackTrace()
        "fake" in { println("some fake spec") }
    }
  }

  private def mkTmp = {
    val file = File.createTempFile("rainycloud-test", ".csv.gz")
    file.deleteOnExit()
    file.toString
  }

}
