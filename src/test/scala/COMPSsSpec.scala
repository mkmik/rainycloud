package it.cnr.aquamaps

import org.specs._

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

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

  "guice compss wrapper" should {
    case class TestModule() extends AbstractModule with ScalaModule {
      def configure() {
        val writer: FileSystemTableWriter[HSPEC] = new FileSystemTableWriter(mkTmp)

        bind[TableWriter[HSPEC]].toInstance(writer)
        bind[FileSystemTableWriter[HSPEC]].toInstance(writer)

        bind[FileParamsGenerator].to[SimpleFileParamsGenerator]
        bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]]
        bind[Emitter[HSPEC]].to[CSVEmitter[HSPEC]]

        bind[Generator].toInstance(mock[Generator])
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

  "static compss wrapper" should {
    case class TestModule() extends AbstractModule with ScalaModule {
      def configure() {
        val writer: FileSystemTableWriter[HSPEC] = new FileSystemTableWriter(mkTmp)

        bind[TableWriter[HSPEC]].toInstance(writer)
        bind[FileSystemTableWriter[HSPEC]].toInstance(writer)

        bind[FileParamsGenerator].to[StaticFileParamsGenerator]
        bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]]
        bind[Emitter[HSPEC]].toInstance(mock[Emitter[HSPEC]])

        bind[Generator].toInstance(mock[Generator])
      }
    }

    val injector = Guice createInjector TestModule()

    val compss = injector.instance[COMPSsGenerator]
    val emitter = injector.instance[Emitter[HSPEC]]

    "pass params in files" in {
      val partition = new Partition("1000", 231)

      compss.computeInPartition(partition)

      there was atLeastOne(emitter).emit(anyObject())
    }
  }

  private def mkTmp = {
    val file = File.createTempFile("rainycloud-test", ".csv.gz")
    file.deleteOnExit()
    file.toString
  }

}
