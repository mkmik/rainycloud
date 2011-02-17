package it.cnr.aquamaps

import com.google.inject._
import scala.xml.{ Node, Text, XML }
import scala.xml.Utility.trim
import io.Source.fromFile
import java.io.File

/*!# COMPSs support

 COMPSs currently only support files as communication method between remote spawns.

*/

/*! Hencefore we needa way to serialize Partitions to and from xml: this import adds the `toXml` and `toPartition` methods to `Partition` and `xml.Node` respectively, see bottom. */
import P2XML._


/*! In order to connect to the rest of the system, first we implement the `Generator` interface. We receive partitions from the entry point here, convert the parameters
 * into files, amd delegate to another interface whose signature COMPSs knowns how to handle (files as parameters). */
class COMPSsGenerator @Inject() (val delegate: FileParamsGenerator) extends Generator {

  def computeInPartition(p: Partition) {
    val tmpFile = mkTmp
    XML.save(tmpFile, p.toXml)

    delegate.computeInPartition(tmpFile)
  }

  def mkTmp = {
    val file = File.createTempFile("rainycloud", ".xml")
    file.deleteOnExit()
    file.toString
  }
}

/*! Now here's the magic. This method accepts files and converts them back to our native parameters and delegates to another Generator (I still don't know if we have to pass java `Files` or file names.) */
trait FileParamsGenerator  {
  val delegate: Generator

  def computeInPartition(fileName: String) {
    delegate.computeInPartition(XML.load(fileName).toPartition)
  }
}

/*! The `FileParamsGenerator` above is just an abstract trait, we need a way to find the backend generator.
 If we are running within a real application it's easy: just let Guice inject it! */
class SimpleFileParamsGenerator @Inject() (val delegate: Generator) extends FileParamsGenerator



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
