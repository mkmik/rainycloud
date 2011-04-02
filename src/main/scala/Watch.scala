package it.cnr.aquamaps

import stopwatch.web.Server

import stopwatch.Stopwatch
import stopwatch.StopwatchGroup
import stopwatch.StopwatchRange
import stopwatch.TimeUnit._
import net.lag.logging.Logger


/*! This is a nice and easy to use utility to take measurements of code execution.
 
  It has been disabled
 until we have a way to allocate a tcp port for each worker. I don't want to put the burdain of
 allocating a port number in a configuration file, just to be able to run more instances of this toy.
 
 VisualVM sampling can be useful to monitor the performance of the code without this kind of interference at source level.
 */
trait Watch {
  // disabled because it opens a tcp port and it conflicts with
  // multiple instances

  // Watch.run

  Stopwatch.enabled = true
  Stopwatch.range = StopwatchRange(0 seconds, 15 seconds, 500 millis)
}

object Watch {
  private val log = Logger(classOf[Watch])

  val server = new stopwatch.web.Server

  def timed[A](caption: String)(body: => A) = {
    val start = System.currentTimeMillis
    log.info("executing %s".format(caption))
    val res = body
    log.info("executed %s in %sms".format(caption, System.currentTimeMillis - start))
    res
  }

  def run = {
    // register StopwatchGroups you want to monitor
    server.groups ::= Stopwatch

    // configure port number
    server.port = 9999
    try {
      //		server.start()
    } catch {
      case _ =>
        log.error("couldn't start stopwatch web monitoring")
    }
  }
}
