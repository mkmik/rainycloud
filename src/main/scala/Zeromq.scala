package it.cnr.aquamaps

import org.zeromq.ZMQ
import scala.actors.Actor._
import scala.actors.Scheduler
import scala.actors.scheduler.ResizableThreadPoolScheduler

object Zeromq {
  println("initializing zeromq")
  val context = ZMQ.context(1)
  System.setProperty("actors.enableForkJoin", "false")
//  Scheduler.impl = new ResizableThreadPoolScheduler()

  implicit def string2bytes(x: String): Array[Byte] = x.getBytes
  implicit def runnable(f: () => Unit): Runnable =
    new Runnable() { def run() = f() }
}

case class SendMessage(val msg: String)

trait ZeromqHandler {
  import Zeromq._

  val socket: ZMQ.Socket

  val handler = actor {
    while(true) {
      receive {
        case SendMessage(msg) =>
          socket.send(socket.getIdentity(), ZMQ.SNDMORE)
          socket.send("", ZMQ.SNDMORE)
          socket.send(msg, 0)      
      }
    }
  }
}

class PirateClient {
  import Zeromq._

  val socket = context.socket(ZMQ.XREQ)
  socket.bind("tcp://*:5566")

  val poller = context.poller()
  poller.register(socket)

  val mq = actor {
    while(true) {
      val res = poller.poll()

      val address = socket.recv(0)
      val delim = socket.recv(0)
      val msg = socket.recv(0)

      println("Client got some message %s from %s".format(new String(msg), new String(address)))
    }
  }
}


case class SendHeartbeat()
case class SendReady()

class PirateWorker(val name: String) extends ZeromqHandler {
  import Zeromq._

  val socket = context.socket(ZMQ.XREQ)
  socket.setIdentity(name)
  socket.connect("tcp://localhost:5566")

  val mq = actor {
    while(true) {
      receive {
        case SendReady() =>
          println("%s sending ready to %s".format(name, handler))
          handler ! SendMessage("READY")
        case SendHeartbeat() =>
          println("%s sending heartbeat to %s".format(name, handler))
          handler ! SendMessage("HEARTBEAT")
        case SendMessage(msg) =>
          println("%s requesting message %s to %s".format(name, msg, handler))
          handler ! SendMessage(msg)
      }
    }
  }

  val businness = actor {
    println("starting %s".format(name))
    mq ! SendReady()

    loop {
      receiveWithin(2000) {
        case "ok" =>
          println("got something")
        case _ =>
          println("timed out %s".format(name))
        mq ! SendHeartbeat()
      }
    }
  }
}

object ZeromqTest extends App {
  val pc = new PirateClient()

  val pw1 = new PirateWorker("w1")
  Thread.sleep(1000)
  val pw2 = new PirateWorker("w2")

  println("started")
}
