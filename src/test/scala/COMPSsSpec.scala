package it.cnr.aquamaps

import org.specs._

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

import org.specs.mock.Mockito
import org.mockito.Matchers._ // to use matchers like anyInt()

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

  "compss wrapper" should {
    case class TestModule() extends AbstractModule with ScalaModule {
      def configure() {
        bind[FileParamsGenerator].to[SimpleFileParamsGenerator]

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
}
