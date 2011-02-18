package it.cnr.aquamaps

/*!## Application Entry point

 This is the application global entrypoint.

 The basic algorithm is very simple: for each partition invoke the 'computeInPartition' method of the `Generator` instance.
 
 The partition is described by a key range. The generator then fetches a number of `HCAF` records according to the specified partition
 and executes an `HspecAlgorithm` for each pair of `HSPEC` x `HSPEN` records, which are emitted by an emitter. See [data model](Tables.scala.html).

 There may be different implementations of `Generator`, for example one which puts request in a queue, and the real generator is a pool of workers
 consuming the queue; or a plain local instance which executes the work locally (and perhaps transparently remotized by COMPSs)
 
 See docs for [Generator](Generator.scala.html#section-1), [HspecAlgorithm](Algorithm.scala.html) and [Emitter](Generator.scala.html#section-10), for further details.
 */

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.util.Modules

class EntryPoint @Inject() (
  val partitioner: Partitioner,
  val generator: Generator,
  val emitter: Emitter[HSPEC]) {

  def run = {
    for (p <- partitioner.partitions)
      generator.computeInPartition(p)

    emitter.flush
  }
}

/*!## Guice startup
 
  This is the java `main` method. It instantiated a fully configured entrypoint with Guice, and runs it. */
object Main {
  def main(argv: Array[String]) = {
    //val injector = Guice createInjector AquamapsModule()
    val injector = Guice createInjector (Modules `override` AquamapsModule() `with` COMPSsModule())  

    val entryPoint = injector.instance[EntryPoint]

    entryPoint.run
    println("exiting")
    System.exit(0)
    println("exiting 2")
  }
}
