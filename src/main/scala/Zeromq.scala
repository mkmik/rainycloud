package it.cnr.aquamaps

import org.zeromq.ZMQ
import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.TIMEOUT
import scala.actors.Futures._
import scala.actors.Channel
import scala.actors.Scheduler
import scala.actors.scheduler.ResizableThreadPoolScheduler

import scala.collection.immutable.HashMap
import scala.collection.immutable.Queue

object Zeromq {
  println("initializing zeromq")
  val context = ZMQ.context(1)
  System.setProperty("actors.enableForkJoin", "false")
  Scheduler.impl = new ResizableThreadPoolScheduler()


  implicit def string2bytes(x: String): Array[Byte] = x.getBytes
  implicit def runnable(f: () => Unit): Runnable =
    new Runnable() { def run() = f() }

  val HEARTBEAT_TIME = 200 * 5 * 10

  case class Job(val body: String) {
    override def toString = body
  }

  case class Worker(val name: String) {
    override def toString = name
  }
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
    //throw new RuntimeException("shouldn't be here")
    return ""
  }

  def sendParts(parts: Array[Byte]*): Unit = sendParts(socket, parts: _*)

  def sendParts(socket: ZMQ.Socket, parts: Array[Byte]*) = {
                
    for (i <- 0 until parts.length) {
      val more = if (i == parts.length - 1) 0 else ZMQ.SNDMORE
//      println("  %s (%s)".format(new String(parts(i)), more))

      socket.send(parts(i), more)
    }
//    println("  ---------")
  }


  def recv() = new String(socket.recv(0))

}

class PirateClient extends ZeromqHandler {
  import Zeromq._

  val socket = context.socket(ZMQ.XREP)
  socket.bind("inproc://client")
  socket.bind("tcp://*:5566")

  /*# Status feedback events */
  case class Ready(val worker: Worker)
  //  case class Rejected(val job: Job)
  case class Died(val worker: Worker)
  case class Joined(val worker: Worker)

  /*# This "actor" implements the communication between the submission client and the workers.
   It handles worker registration, heartbeating, job book keeping. Higher level job handling is
   done in the 'sender' actor. */
  val mq = future {
    val poller = context.poller()
    poller.register(socket, ZMQ.Poller.POLLIN)

    var workers = new HashMap[Worker, Long]()

    def workerReady(worker: Worker) = {
      workerAlive(worker)

      sender ! Ready(worker)
    }

    def workerAlive(worker: Worker) = {
      println("%s still alive".format(worker))
      if (!workers.contains(worker))
        sender ! Joined(worker)

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

    def submitJob(worker: Worker, job: Job) {
      sendParts(worker.name, socket.getIdentity(), "", "SUBMIT", job.toString)
    }

    while (true) {
      val events = poller.poll(500000)
      if (events > 0) {
        val worker = Worker(getAddress())
        val msg = recv()

        msg match {
          case "READY" => workerReady(worker)
          case "HEARTBEAT" => workerAlive(worker)
          case "KILL" => sendParts(recv(), socket.getIdentity(), "", "KILL")
          case "SUBMIT" => submitJob(worker, Job(recv()))
        }
      }

      checkDeaths()
    }
  }

  //

  // commands for 'sender' actor
  case class Submit(val job: String)
  case class Kill(val worker: String)

  /*# This actor implements the API between the submission API users and the zmq subsystem.
   Furthermore it handles the job rejection and retries. */
  val sender = actor {
    val socket = context.socket(ZMQ.XREQ)
    socket.connect("inproc://client")

    var queuedJobs = Queue[Job]()
    var readyWorkers = Queue[Worker]()

    def submit(job: Job): Unit = {
      queuedJobs = queuedJobs enqueue job
      flushQueue()
    }

    def flushQueue() = {
      val slots = readyWorkers zip queuedJobs
      println("FLUSHING QUEUE ready workers: %s, queuedJobs: %s, matched slot size: %s".format(readyWorkers.size, queuedJobs.size, slots.size))
      for((worker, job) <- slots) {
        println("can run %s on %s".format(job, worker))
        sendToWorker(worker, job)
      }
      queuedJobs = queuedJobs drop (slots.size)
      readyWorkers = readyWorkers drop (slots.size)
    }

    def sendToWorker(worker: Worker, job: Job): Unit = sendParts(socket, worker.name, "", "SUBMIT", job.toString)

    while (true) {
      receive {
        //        case Rejected(job) => println("queue is full, job is rejected")
        case Ready(worker) => readyWorkers = readyWorkers enqueue worker
        case Died(worker) => println("worker '%s' is dead while holding running".format(worker))
        case Joined(worker) => println("worker joined %s".format(worker))
        case Kill(worker) => sendParts(socket, "dummy", "", "KILL", worker)
        case Submit(job) => submit(Job(job))
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
//  socket.bind("inproc://worker-" + name)

  val mq = future {
//    println("sending ready from %s".format(name))
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
              worker ! Submit(Job(job))
              println("worker actor messaged")
          }

        }

        send("HEARTBEAT")
      }
    }

    eventLoop()
    println("W %s died".format(name))
  }

  case class Submit(val job: Job)
  case class Finish()

  val worker = actor {
    println("worker INNERT " + name)

    val socket = context.socket(ZMQ.XREQ)
//    socket.setIdentity(name+"_b")
    socket.connect("tcp://localhost:5566")

    while (true) {
//    loop {
      receiveWithin(2000) {
        case Submit(job) => execute(job)
        case Finish() => println("sending back ready"); send(socket, "READY")
        case TIMEOUT => println("ww %s timed out".format(name))
      }
    }
  }

  def execute(job: Job): Unit = future {
    println("W %s is working for real (about 6 sec)")
    Thread.sleep(6000)
    println("W %s finished computing")
    worker ! Finish()
  }

  def send(msg: String) : Unit = send(socket, msg)
  def send(socket: ZMQ.Socket, msg: String) = sendParts(socket.getIdentity(), "", msg)
}

object ZeromqTest extends App {
  val pc = new PirateClient()

  def startWorkers() = {
    val pw1 = new PirateWorker("w1")
//    Thread.sleep(1000)
    val pw2 = new PirateWorker("w2")
  }

  startWorkers()

  Thread.sleep(5000)
  println("SENDING TEST COMMAND to w1")

  for (i <- 1 to 3) {
    pc.dispatch("Test Command %s".format(i))
  }

/*  Thread.sleep(4000)
  startWorkers()

  Thread.sleep(5000)
  pc.kill("w2")

  Thread.sleep(12000)

  for (i <- 1 to 10) {
    pc.dispatch("Test Command %s".format(i))
  }

  Thread.sleep(10000)
  val pw3 = new PirateWorker("w3")
*/

  pc.mq()
}
