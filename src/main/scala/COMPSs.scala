package it.cnr.aquamaps

import com.google.inject._
import scala.xml.{ Node, Text, XML }
import scala.xml.Utility.trim
import io.Source.fromFile
import java.io.File
import com.google.inject._
import com.google.inject.util.Modules
import uk.me.lings.scalaguice.InjectorExtensions._
import uk.me.lings.scalaguice.ScalaModule
import org.guiceyfruit.Injectors

/*!# COMPSs support

 COMPSs currently only support files as communication method between remote spawns.

*/

/*! Hencefore we needa way to serialize Partitions to and from xml: this import adds the `toXml` and `toPartition` methods to `Partition` and `xml.Node` respectively, see bottom. */
import P2XML._

/*! In order to connect to the rest of the system, first we implement the `Generator` interface. We receive partitions from the entry point here, convert the parameters
 * into files, amd delegate to another interface whose signature COMPSs knowns how to handle (files as parameters). */
class COMPSsGenerator @Inject() (val delegate: FileParamsGenerator, val emitter: Emitter[HSPEC]) extends Generator {

  def computeInPartition(p: Partition) {
    val tmpFile = mkTmp
    XML.save(tmpFile, p.toXml)

    val outputFile = delegate.computeInPartition(tmpFile)

    println("got generated HSPEC records in %s; merging results".format(outputFile))

    merge(outputFile)
  }

  /*! Well this is a rather stupid way to merge the remote output into our single result. `Emitter` should be extended to support bulk emits. */
  def merge(outputFile: String) {
    val loader = new TableHSPECLoader(new CSVPositionalSource(new FileSystemTableReader(outputFile)))

    for(hspec <- loader.load)
      emitter.emit(hspec)
  }

  def mkTmp = {
    val file = File.createTempFile("rainycloud", ".xml")
    file.deleteOnExit()
    file.toString
  }
}

/*! Now here's the magic. This method accepts files and converts them back to our native parameters and delegates to another Generator (I still don't know if we have to pass java `Files` or file names.) */
trait FileParamsGenerator {
  def computeInPartition(fileName: String): String
}

/*! The `FileParamsGenerator` above is just an abstract trait, we need a way to find the backend generator.
 If we are running within a real application it's easy: just let Guice inject it! */
class SimpleFileParamsGenerator @Inject() (val delegate: Generator, val emitter: Emitter[HSPEC], val writer: FileSystemTableWriter[HSPEC]) extends FileParamsGenerator {
  def computeInPartition(fileName: String): String = {
    delegate.computeInPartition(XML.load(fileName).toPartition)
    emitter.flush
    writer.name
  }
}

/*! But what if COMPSs requires only static method invocations because it wouldn't know how to spawn the instances on the remote worker ? */
class StaticFileParamsGenerator extends FileParamsGenerator {
  def computeInPartition(fileName: String): String = StaticFileParamsGenerator.staticDelegate(fileName)
}

/*! The static delegate also returns the output filename so that COMPSs can move the data for us. */
object StaticFileParamsGenerator {

  case class COMPSsWorkerModule() extends AbstractModule with ScalaModule {
    def configure() {
      /*! Let's write to a temporary file, so that the same machine can host several instances of this worker. In order to do this
       we override the Guice config and inject another TableWriter. */
      val writer: FileSystemTableWriter[HSPEC] = new FileSystemTableWriter(mkTmp)

      bind[TableWriter[HSPEC]].toInstance(writer)
      bind[FileSystemTableWriter[HSPEC]].toInstance(writer)
    }

    def mkTmp = {
      val file = File.createTempFile("rainycloud-worker-", ".csv.gz")
      file.deleteOnExit()
      file.toString
    }
  }

  // val injector = Guice createInjector (Modules `override` AquamapsModule() `with` COMPSsWorkerModule())  

  def staticDelegate(fileName: String): String = {
    
    // should be moved outside, but right now we have an issue with emitter singletons
    val injector = Guice createInjector (Modules `override` AquamapsModule() `with` COMPSsWorkerModule())  

    val writer = injector.instance[FileSystemTableWriter[HSPEC]]
    val generator = injector.instance[Generator]
    val emitter = injector.instance[Emitter[HSPEC]]

    generator.computeInPartition(XML.load(fileName).toPartition)
    emitter.flush
    println("closing injector")
    Injectors.close(injector)
    writer.name
  }
}

/*!## Serialization

 These are details, we need some (de)serialization for partition descriptors. Let's go for a readable choice: */

class P2XML(val p: Partition) {
  def toXml() = {
    <partition>
      <start>{ p.start }</start>
      <size>{ p.size }</size>
    </partition>
  }
}

class XML2P(val p: Node) {
  def toPartition: Partition = trim(p) match {
    case <partition><start>{ start }</start><size>{ size }</size></partition> => new Partition(start.text, size.text.toLong)
  }
}

object P2XML {
  implicit def p2xml(p: Partition): P2XML = new P2XML(p)
  implicit def p2xml(p: Node): XML2P = new XML2P(p)
}
