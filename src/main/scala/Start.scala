package it.cnr.aquamaps

/*!## Application Entry point

 This is the application global entrypoint.

 The basic algorithm is very simple: for each partition invoke the 'computeInPartition' method of the Generator instance.
 
 The partition is described by a key range. The generator then fetches a number of HCAF records according to the specified partition
 and executes an HspecAlgorithm for each pair of HSPEC x HSPEN records, which are emitted by an emitter.

 There may be different implementations of Generator, for example one which puts request in a queue, and the real generator is a pool of workers
 consuming the queue; or a plain local instance which executes the work locally (and perhaps transparently remotized by COMPSs)
 
 See docs for Generator, HspecAlgorithm and Emitter, for further details.
 */

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._

class EntryPoint @Inject() (
  val partitioner: Partitioner,
  val generator: Generator) {

  def run = for (p <- partitioner.partitions) generator.computeInPartition(p)
}

/*!## Guice startup
 
  This is the java main method. It instantiated a fully configured entrypoint with guice, and runs it */
object Main {
  def main(argv: Array[String]) = {
    val injector = Guice createInjector AquamapsModule()
    val entryPoint = injector.instance[EntryPoint]

    entryPoint.run
  }
}