package it.cnr.aquamaps

import com.google.inject.Guice
import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._

/** Entry point of the application */
class EntryPoint @Inject() (
  val partitioner: Partitioner,
  val generator: Generator) {

  def run = for (p <- partitioner.partitions) generator.computeInPartition(p)
}

/** Main. Creates a configured entrypoint with guice, and runs it */
object Main {
  def main(argv: Array[String]) = {
    val injector = Guice createInjector AquamapsModule()
    val entryPoint = injector.instance[EntryPoint]

    entryPoint.run
  }
}
