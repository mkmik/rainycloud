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
import com.google.inject.util.{ Modules => GuiceModules }
import net.lag.configgy.{ Config, Configgy }
import net.lag.logging.Logger
import scopt.OptionParser
import scopt.OptionParser._

import javax.servlet.Servlet

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
class Main
object Main {
  private val log = Logger(classOf[Main])

  val parser = new OptionParser("scopt") {
    opt("s", "storage", "storage type (local, hdfs)", { v: String => Configgy.config.setString("storage", v) })
    opt("r", "ranges", "range file", { v: String => Configgy.config.setString("ranges", v) })
    opt("e", "inlineranges", "inline ranges", { v: String => Configgy.config.setList("inlineranges", v.split(",")) })
    opt("hcaf", "hcaf file", { v: String => Configgy.config.setString("hcafFile", v) })
    opt("hspen", "hspen file", { v: String => Configgy.config.setString("hspenFile", v) })
    opt("hspec", "hspec file", { v: String => Configgy.config.setString("hspecFile", v) })
    opt("m", "module", "add a module to runtime", { v: String => val c = Configgy.config; c.setList("modules", (c.getList("modules").toList ++ List(v)).distinct) })
    opt("w", "worker", "run a worker", { Configgy.config.setBool("worker", true) })
    intOpt("worker-agony", "time until a worker dies (usefult for takeover testing)", { v: Int => Configgy.config.setInt("worker-agony", v) })
    opt("submitter", "run a submitter", { Configgy.config.setBool("submitter", true) })
    opt("web", "run a web server for monitoring, submission interface etc", { Configgy.config.setBool("web", true) })
    intOpt("port", "port for the web server", { v: Int => Configgy.config.setInt("web-port", v) })
    intOpt("queue-port", "port for the queue", { v: Int => Configgy.config.setInt("queue-port", v) })
    opt("queue-host", "host for the queue (workers connects to this address)", { v: String => Configgy.config.setString("queue-host", v) })
  }

  def parseArgs(args: Array[String]) = parser.parse(args)

  def main(args: Array[String]) {
    val conf = loadConfiguration

    if (!parser.parse(args))
      return


    if (conf.getBool("web").getOrElse(false)) {
      conf.setList("modules", "Web" :: conf.getList("modules").toList)
    }

    val injector = createInjector(conf)

    //  "/submitter/socket.io/*" -> new ZeromqMonitoringSocket()
    if (conf.getBool("web").getOrElse(false)) {
      //WebServer.run(Seq("/submitter/*" -> new ZeromqMonitoring(), "/socket.io/*" -> new ZeromqMonitoringSocket()))

      val submitterApi = injector.instance[cloud.SubmitterApi]
      val zeromqMonitoring = injector.instance[ZeromqMonitoring]

      WebServer.run(Seq("/submitter/*" -> zeromqMonitoring, "/api/*" -> submitterApi))
//      WebServer.run(Seq("/submitter/*" -> new ZeromqMonitoring()))
      // WebServer.run(Seq("/socket.io/*" -> new ZeromqMonitoringSocket()), Some(8781))
    }

    if (conf.getBool("worker").getOrElse(false)) {
      cloud.Worker.main(args)
      return
    }

/*
    if (conf.getBool("submitter").getOrElse(false)) {
      cloud.SubmitterTester.main(args)
      return
    }
*/

    if (!conf.getBool("web").getOrElse(false)) {
      val entryPoint = injector.instance[EntryPoint]
      entryPoint.run

      cleanup(injector)
    }
  }

  def createInjector(conf: Config) = {
    val mods = Modules.enabledModules(conf)
    printSomeFeedback(mods, conf)

    /*! Configure our Guice context with the main modules overrided with optional modules obtained from config file + cmdline. */
    Guice createInjector (GuiceModules `override` AquamapsModule() `with` (mods: _*))
  }

  def loadConfiguration = {
    Configgy.configure("rainycloud.conf")
    Configgy.config
  }

  def printSomeFeedback(mods: Seq[Module], conf: Config) {
    log.info("Available modules: %s".format(Modules.modules.values.mkString(", ")))
    log.info("Enabled modules: %s".format(mods.mkString(", ")))
  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    log.info("done")
    injector.instance[Fetcher[HCAF]].shutdown
    injector.instance[Loader[HSPEN]].shutdown
  }
}
