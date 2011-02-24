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
import com.google.inject.util.{Modules => GuiceModules}
import net.lag.configgy.{Config, Configgy}
import org.github.scopt.OptionParser
import org.github.scopt.OptionParser._
import java.io.File

trait EntryPoint {
  def run
}

class SimpleEntryPoint @Inject() (
  val partitioner: Partitioner,
  val generator: Generator,
  val emitter: Emitter[HSPEC]) extends EntryPoint {

  def run = {
    for (p <- partitioner.partitions)
      generator.computeInPartition(p)

    emitter.flush
  }
}

/*!## Guice startup
 
  This is the java `main` method. It instantiated a fully configured entrypoint with Guice, and runs it. */
object Main {

  def main(args: Array[String]) {
    val conf = loadConfiguration

    if (!parser.parse(args)) 
      return

    val injector = createInjector(conf)

    val entryPoint = injector.instance[EntryPoint]
    entryPoint.run

    cleanup(injector)
  }

  val parser = new OptionParser("scopt") {
    opt("s", "storage", "storage type (local, hdfs)", {v: String => Configgy.config.setString("storage", v)}) // TODO: this won't work! includes are already processed %!@#$
    opt("r", "ranges", "range file", {v: String => Configgy.config.setString("ranges", v)})
    opt("m", "module", "add a module to runtime", {v: String => val c= Configgy.config; c.setList("modules", (c.getList("modules").toList ++ List(v)).distinct) })
  }

  def createInjector(conf: Config) = {
    val mods = Modules.enabledModules(conf)
    printSomeFeedback(mods, conf)

    /*! Configure our Guice context with the main modules overrided with optional modules obtained from config file + cmdline. */    
    Guice createInjector (GuiceModules `override` AquamapsModule() `with` (mods :_*))
  }

  def loadConfiguration = {
    if(new File("rainycloud.conf").exists())
      Configgy.configure("rainycloud.conf")
    else
      Configgy.configureFromString("")
    Configgy.config
  }

  def printSomeFeedback(mods: Seq[Module], conf: Config) {
    println("Available modules: %s".format(Modules.modules.values.mkString(", ")))
    println("Enabled modules: %s".format(mods.mkString(", ")))
  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    println("done")
    injector.instance[Fetcher[HCAF]].shutdown
    injector.instance[Loader[HSPEN]].shutdown
  }
}
