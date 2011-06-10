package it.cnr.aquamaps

import org.zeromq.ZMQ
import scala.actors.Actor
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
case class SendMessageTo(val address: String, val msg: String)
case class RecvMessage(val address: String, val msg: String)

trait ZeromqHandler {
  import Zeromq._

  val socket: ZMQ.Socket
  val mq : Actor

  def getAddress() : String = {
    var data = socket.recv(0)
    while(socket.hasReceiveMore()) {
      val next = socket.recv(0)
      if (next.length == 0) {
//        println("          Delimiter reached returning '%s'".format(new String(data)))
        return new String(data)
      }
      data = next
    }
    println("shouldn't be here")
    return "unknown"
  }

  val handler = actor {
    val poller = context.poller()
    poller.register(socket, ZMQ.Poller.POLLIN)

    while(true) {
      val events = poller.poll(200000)
      if (events > 0) {
        println("poller says we have some data %s".format(events))
        val address = getAddress()
        val msg = socket.recv(0)

        println("MESSAGE RECEIVED '%s' from '%s'".format(new String(msg), address))
        mq ! RecvMessage(new String(address), new String(msg))
      }

      receiveWithin(200) {
        case SendMessage(msg) =>
          println("%s sends a message '%s'".format(this, msg))
          socket.send(socket.getIdentity(), ZMQ.SNDMORE)
          socket.send("", ZMQ.SNDMORE)
          socket.send(msg, 0)
        case SendMessageTo(address, msg) =>
          println("%s sends a message '%s' to %s".format(this, msg, address))
          socket.send(address, ZMQ.SNDMORE)
          socket.send(socket.getIdentity(), ZMQ.SNDMORE)
          socket.send("", ZMQ.SNDMORE)
          socket.send(msg, 0)
        case _ =>
          
      }
    }
  }
}

class PirateClient extends ZeromqHandler {
  import Zeromq._

  val socket = context.socket(ZMQ.XREP)
  socket.bind("tcp://*:5566")

  val mq = actor {
    while(true) {
      receive {
        case RecvMessage(address, msg) =>
          println("Client got message '%s'".format(msg))
      }
    }
  }

  def dispatch(address: String, msg: String) = {
    handler ! SendMessageTo(address, msg)
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
          println("%s sending ready".format(name))
          handler ! SendMessage("READY")
        case SendHeartbeat() =>
          println("%s sending heartbeat".format(name))
          handler ! SendMessage("HEARTBEAT")
        case SendMessage(msg) =>
          println("%s requesting message %s".format(name, msg))
          handler ! SendMessage(msg)
        case RecvMessage(address, msg) =>
          println("%s GOT REQUEST %s from %s".format(name, msg, address))
          handler ! SendMessage(msg)
      }
    }
  }

  val businness = actor {
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

  Thread.sleep(5000)
  println("SENDING TEST COMMAND to w1")
  pc.dispatch("w1", "test command")

  println("started")
}
