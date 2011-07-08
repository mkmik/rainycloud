package it.cnr.aquamaps.cloud

import it.cnr.aquamaps._

import java.util.UUID
import java.net.InetAddress

import net.lag.logging.Logger
import net.lag.configgy.{ Config, Configgy }

object Worker extends App {
  private val log = Logger(Worker getClass)

  log.info("starting worker")

  val hostname = InetAddress.getLocalHost.getHostName

  val worker = new ZeromqTaskExecutor("%s-%s".format(hostname, UUID.randomUUID.toString))

  log.info("ok, worker started")

  Configgy.config.getInt("worker-agony") match {
    case Some(agony) =>
      log.info("wait %s and then will die".format(agony))
      Thread.sleep(agony * 1000)
      log.info("dying for testing")
      System.exit(0)
    case None =>
  }

}

