package it.cnr.aquamaps

import com.google.inject._
import scala.xml.{ Node, Text, XML }
import scala.xml.Utility.trim
import io.Source.fromFile
import java.io.File
import com.google.inject._
import com.google.inject.util.{ Modules => GuiceModules }
import uk.me.lings.scalaguice.InjectorExtensions._
import uk.me.lings.scalaguice.ScalaModule
import Watch.timed
import net.lag.logging.Logger
import java.io._
import org.apache.commons.io.IOUtils
import resource._
import collection.mutable.ArrayBuffer


/*!# COMPSs object passing support.

 COMPSs now supports also java objects as communication method between remote spawns.

*/
class COMPSsObjectGenerator @Inject() (val delegate: ObjectParamsGenerator[HSPEC], val emitter: Emitter[HSPEC]) extends Generator {
  def computeInPartition(p: Partition) {
    val records = delegate.computeInPartition(p)
    for(r <- records)
      emitter.emit(r)
  }
}

trait ObjectParamsGenerator[A] {
 def computeInPartition(p: Partition): Array[A]
}

/*! But what if COMPSs requires only static method invocations because it wouldn't know how to spawn the instances on the remote worker ? */
class StaticObjectParamsGenerator extends ObjectParamsGenerator[HSPEC] {
  def computeInPartition(p: Partition) = StaticObjectParamsGenerator.staticDelegate(p)
}

class MemoryEmitter[A] @Inject() (val records: ArrayBuffer[A]) extends Emitter[A] {

  def emit(record: A) {
    records += record
  }

  def flush {
    
  }
}

/*! The static delegate also returns an array of HSPECs so that COMPSs can move the data for us. */
object StaticObjectParamsGenerator {
  case class COMPSsWorkerModule() extends AbstractModule with ScalaModule {
    def configure() {
      bind[ArrayBuffer[HSPEC]].in[Singleton]
      bind[Emitter[HSPEC]].to[MemoryEmitter[HSPEC]]
    }
  }

  def staticDelegate(p: Partition) = {
    withInjector { injector =>
      val generator = injector.instance[Generator]
      generator.computeInPartition(p)

      val buffer = injector.instance[ArrayBuffer[HSPEC]]
      buffer.toArray
    }
  }

  def withInjector[A](body: Injector => A) = {
    /*! We have to create a new DI context, since we run in a static method (and possibly on another machine, in a completely disconnected runtime context) */
    val i = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (COMPSsWorkerModule(), COMPSsWorkerHDFSModule(), RandomAlgoModule()))
    val res = body(i)
    i.instance[Fetcher[HCAF]].shutdown
    i.instance[Loader[HSPEN]].shutdown
    res
  }
}
