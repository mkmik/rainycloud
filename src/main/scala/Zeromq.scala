package it.cnr.aquamaps

import org.zeromq.ZMQ
import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.TIMEOUT
import scala.actors.Futures._
import scala.actors.Channel
import scala.actors.Scheduler
import scala.actors.scheduler.ResizableThreadPoolScheduler

import scala.collection.immutable.TreeMap
import scala.collection.immutable.Queue

object Zeromq {
  println("initializing zeromq")
  val context = ZMQ.context(1)
  System.setProperty("actors.enableForkJoin", "false")

  implicit def string2bytes(x: String): Array[Byte] = x.getBytes
  implicit def runnable(f: () => Unit): Runnable =
    new Runnable() { def run() = f() }

  val HEARTBEAT_TIME = 200 * 5 * 10
}

trait ZeromqHandler {
  import Zeromq._

  val socket: ZMQ.Socket

  def getAddress(): String = {
    var data = socket.recv(0)
    while (socket.hasReceiveMore()) {
      val next = socket.recv(0)
      if (next.length == 0) {
        return new String(data)
      }
      data = next
    }
    throw new RuntimeException("shouldn't be here")
  }

  def sendParts(parts: Array[Byte]*): Unit = sendParts(socket, parts: _*)

  def sendParts(socket: ZMQ.Socket, parts: Array[Byte]*) = {
    for (i <- 0 until parts.length) {
      val more = if (i == parts.length - 1) 0 else ZMQ.SNDMORE
      socket.send(parts(i), more)
    }
  }

  def recv() = new String(socket.recv(0))

}

class PirateClient extends ZeromqHandler {
  import Zeromq._

  val socket = context.socket(ZMQ.XREP)
  socket.bind("inproc://client")
  socket.bind("tcp://*:5566")

  
  /*# Status feedback events */
  case class Rejected(val job: String)
  case class Died(val worker: String)
  case class Joined(val worker: String)

  /*# This "actor" implements the communication between the submission client and the workers.
   It handles worker registration, heartbeating, job book keeping. Higher level job handling is
   done in the 'sender' actor. */
  val mq = future {
    val poller = context.poller()
    poller.register(socket, ZMQ.Poller.POLLIN)

    var workers = new TreeMap[String, Long]()
    var workerNum = 0
    def roundRobinWorker = {
      workerNum += 1
      if (workers.size == 0)
        None
      else
        Some(workers.keys.toIndexedSeq(workerNum % workers.size))
    }

    def workerAlive(worker: String) = {
      if (!workers.contains(worker))
        sender ! Joined(worker: String)

      workers += ((worker, System.currentTimeMillis()))
    }

    def checkDeaths() = {
      val now = System.currentTimeMillis()
      for ((w, t) <- workers) {
        if (now - t > HEARTBEAT_TIME) {
          sender ! Died(w)
          workers -= w
        }
      }
    }

    while (true) {
      val events = poller.poll(200000)
      if (events > 0) {
        val address = getAddress()
        val msg = recv()

        msg match {
          case "READY" => workerAlive(address)
          case "HEARTBEAT" => workerAlive(address)
          case "KILL" => sendParts(recv(), socket.getIdentity(), "", "KILL")
          case "SUBMIT" =>
            //            println("C got submission request on behalf of %s".format(address))
            val job = recv()
            val rw = roundRobinWorker
            println("C SUBMITTING job: '%s' to %s".format(job, rw))
            rw match {
              case Some(worker) => sendParts(worker, socket.getIdentity(), "", "SUBMIT", job)
              case None => sender ! Rejected(job)
            }
        }
      }

      checkDeaths()
    }
  }


  // commands for 'sender' actor
  case class Submit(val job: String)
  case class Kill(val worker: String)

  /*# This actor implements the API between the submission API users and the zmq subsystem.
   Furthermore it handles the job rejection and retries. */
  val sender = actor {
    val socket = context.socket(ZMQ.XREQ)
    socket.connect("inproc://client")

    var queued: Queue[String] = Queue()
    def queueForLater(job: String) = {
      println("job '%s' was rejected, queuing".format(job))
      queued = queued enqueue job
    }

    def retryQueued() = queued size match {
      case 0 => 0
      case _ =>
        val (job, rest) = queued.dequeue
        queued = rest
        println("job '%s' was queued, trying to submit it again".format(job))
        submit(job)
    }

    def submit(job: String) = sendParts(socket, "dummy", "", "SUBMIT", job)

    while (true) {
      receiveWithin(1000) {
        case Rejected(job) => queueForLater(job)
        case Died(worker) => println("worker '%s' is dead while holding running".format(worker))
        case Joined(worker) => println("worker joined %s".format(worker))
        case Kill(worker) => sendParts(socket, "dummy", "", "KILL", worker)
        case Submit(job) => submit(job)
        case TIMEOUT => retryQueued()
      }
    }
  }

  def dispatch(msg: String) = sender ! Submit(msg)
  def kill(worker: String) = sender ! Kill(worker)
}

class PirateWorker(val name: String) extends ZeromqHandler {
  import Zeromq._

  val socket = context.socket(ZMQ.XREQ)
  socket.setIdentity(name)
  socket.connect("tcp://localhost:5566")

  val mq = future {
    send("READY")

    val poller = context.poller()
    poller.register(socket, ZMQ.Poller.POLLIN)

    def eventLoop(): Unit = {
      while (true) {
        val res = poller.poll(1000 * 1000)
        if (res > 0) {

          println("W %s got poll in".format(name))

          val address = getAddress()
          recv() match {
            case "KILL" =>
              println("W %s was shot in the head, dying".format(name))
              return
            case "SUBMIT" =>
              val job = recv()
              println("W %s got submission '%s'".format(name, job))
          }

        }

        send("HEARTBEAT")
      }
    }

    eventLoop()
    println("W %s died".format(name))
  }

  def send(msg: String) = {
    sendParts(socket.getIdentity(), "", msg)
  }
}

object ZeromqTest extends App {
  val pc = new PirateClient()

  def startWorkers() = {
    val pw1 = new PirateWorker("w1")
    Thread.sleep(1000)
    val pw2 = new PirateWorker("w2")
  }

  //  startWorkers()

  Thread.sleep(5000)
  println("SENDING TEST COMMAND to w1")

  for (i <- 1 to 2) {
    pc.dispatch("Test Command %s".format(i))
  }

  Thread.sleep(4000)
  startWorkers()

  Thread.sleep(5000)
  pc.kill("w2")

  Thread.sleep(12000)

  for (i <- 1 to 10) {
    pc.dispatch("Test Command %s".format(i))
  }

  Thread.sleep(10000)
  val pw3 = new PirateWorker("w3")

  pc.mq()
}
