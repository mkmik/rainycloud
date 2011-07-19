package it.cnr.aquamaps.cloud

import it.cnr.aquamaps._

import java.util.UUID
import java.net.InetAddress

import com.weiglewilczek.slf4s.Logging
import net.lag.configgy.{ Config, Configgy }

object Worker extends App with Logging {

  logger.info("starting worker")

  val hostname = InetAddress.getLocalHost.getHostName

  val worker = new ZeromqTaskExecutor("%s-%s".format(hostname, UUID.randomUUID.toString))

  logger.info("ok, worker started")

  Configgy.config.getInt("worker-agony") match {
    case Some(agony) =>
      logger.info("wait %s and then will die".format(agony))
      Thread.sleep(agony * 1000)
      logger.info("dying for testing")
      System.exit(0)
    case None =>
  }

}

