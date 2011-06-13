package it.cnr.aquamaps.cloud

import it.cnr.aquamaps._

import java.util.UUID
import java.net.InetAddress

object Worker extends App {
  println("ok, worker started")

  val hostname = InetAddress.getLocalHost.getHostName

  val worker = new ZeromqTaskExecutor("%s-%s".format(hostname, UUID.randomUUID.toString))
}


