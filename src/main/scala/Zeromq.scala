package it.cnr.aquamaps

import org.zeromq.ZMQ
import scala.actors.Futures._

import akka.actor.Actor
import akka.actor.Actor._
import akka.dispatch.Dispatchers
import akka.util.duration._
import akka.actor.ReceiveTimeout

import scala.collection.immutable.HashMap
import scala.collection.immutable.Queue

import net.lag.logging.Logger

object Zeromq {
  println("initializing zeromq")
  val context = ZMQ.context(1)
  //  System.setProperty("actors.enableForkJoin", "false")
  //  Scheduler.impl = new ResizableThreadPoolScheduler()

  implicit def string2bytes(x: String): Array[Byte] = x.getBytes
  implicit def runnable(f: () => Unit): Runnable =
    new Runnable() { def run() = f() }

  val HEARTBEAT_TIME = 200 * 5 * 10

  /*# Command events */
  case class Submit(val job: Job)
  case class Finish()
  /*# Status feedback events */
  case class Ready(val worker: Worker)
  //  case class Rejected(val job: Job)
  case class Died(val worker: Worker)
  // a node joins when it sends active heartbeat
  case class Joined(val worker: Worker)
  /*# Commands/actions */
  case class Kill(val worker: String)

  case class Job(val body: String) {
    override def toString = body
  }

  case class Worker(val name: String) {
    override def toString = name
  }
}

trait ZeromqHandler {
  import Zeromq._

  private val log = Logger(classOf[ZeromqHandler])

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
    log.debug("  --------- Sending (thread %s):".format(Thread.currentThread()))
    for (i <- 0 until parts.length) {
      val more = if (i == parts.length - 1) 0 else ZMQ.SNDMORE
      log.debug("  '%s' (%s bytes) (%s)".format(new String(parts(i)), parts(i).length, more))

      socket.send(parts(i), more)
    }
    log.debug("  ---------")
  }

  def recv() = new String(socket.recv(0))

}

class PirateClient extends ZeromqHandler {
  import Zeromq._

  private val log = Logger(classOf[PirateClient])

  val socket = context.socket(ZMQ.XREP)
  socket.bind("inproc://client")
  socket.bind("tcp://*:5566")

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
      log.debug("%s still alive".format(worker))
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
          case _ => throw new IllegalArgumentException("unknown command '%s' for worker '%s' in thread %s".format(msg, worker, Thread.currentThread()))
        }
      }

      checkDeaths()
    }
  }

  //

  /*# This actor implements the API between the submission API users and the zmq subsystem.
   Furthermore it handles the job rejection and retries. */
  class SenderActor extends Actor {
    private val log = Logger(classOf[SenderActor])

    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, 5, 100.milliseconds)

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
      log.debug("FLUSHING QUEUE ready workers: %s, queuedJobs: %s, matched slot size: %s".format(readyWorkers.size, queuedJobs.size, slots.size))
      for ((worker, job) <- slots) {
        log.debug("can run %s on %s".format(job, worker))
        sendToWorker(worker, job)
      }

      queuedJobs = queuedJobs drop (slots.size)

      log.debug("ready workers was: %s".format(readyWorkers))
      readyWorkers = readyWorkers drop (slots.size)
      log.debug("ready workers is:  %s".format(readyWorkers))
    }

    def sendToWorker(worker: Worker, job: Job): Unit = sendParts(socket, worker.name, "", "SUBMIT", job.toString)

    def acceptWorker(worker: Worker) = {
      log.debug("Worker %s is now ready.Previous ready workers: %s (%s)".format(worker, readyWorkers, Thread.currentThread()))
      readyWorkers = readyWorkers enqueue worker
      log.debug("                        Current ready workers: %s".format(readyWorkers))
      flushQueue()
    }

    def buryWorker(worker: Worker) = {
      log.warning("worker '%s' is dead while running".format(worker))
      readyWorkers = readyWorkers.filterNot(el => el.name == worker.name)
    }

    def summary() = {
      log.info(" -------> Currently %s workers alive (%s), queue length: %s".format(readyWorkers.length, readyWorkers, queuedJobs.length))
    }

    self.receiveTimeout = Some(4000L)

    def receive = {
      case Ready(worker) => acceptWorker(worker)
      case Joined(worker) => log.info("Worker %s is joined".format(worker))
      case Died(worker) => buryWorker(worker)
      case Kill(worker) => sendParts(socket, "dummy", "", "KILL", worker)
      case Submit(job) => submit(job)
      case ReceiveTimeout => summary()
    }
  }

  val sender = actorOf(new SenderActor()).start

  def dispatch(msg: String) = sender ! Submit(Job(msg))
  def kill(worker: String) = sender ! Kill(worker)
}

class PirateWorker(val name: String) extends ZeromqHandler {
  import Zeromq._

  private val log = Logger(classOf[PirateWorker])

  val socket = context.socket(ZMQ.XREQ)

  val mq = future {
    socket.setIdentity(name)
    socket.connect("tcp://localhost:5566")

    log.debug("sending ready from %s".format(name))
    send("READY")

    val poller = context.poller()
    poller.register(socket, ZMQ.Poller.POLLIN)

    def eventLoop(): Unit = {
      while (true) {
        val res = poller.poll(1000 * 1000)
        if (res > 0) {

          log.debug("W %s got poll in".format(name))

          val address = getAddress()
          recv() match {
            case "KILL" =>
              log.info("W %s was shot in the head, dying".format(name))
              return
            case "SUBMIT" =>
              val job = recv()
              log.debug("W %s got submission '%s'".format(name, job))
              worker ! Submit(Job(job))
              log.debug("worker actor messaged")
          }

        }

        send("HEARTBEAT")
      }
    }

    eventLoop()
    log.warning("W %s died".format(name))
  }

  def executeJob(job: Job): Unit = {
    log.info("W %s will spawn background job for (about 6 sec)".format(name))
    spawn {
      log.debug("W %s is working for real (about 6 sec)".format(name))
      for (i <- 1 to 6) {
        log.debug("W %s is working on step %s of job %s".format(name, i, job))
        Thread.sleep(100)
      }
      log.info("W %s finished computing job %s".format(name, job))
      worker ! Finish()
    }
  }

  class WorkerActor extends Actor {
    private val log = Logger(classOf[WorkerActor])

    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, 5, 100.milliseconds)

    val socket = context.socket(ZMQ.XREQ)

    override def preStart() = {
      log.debug("pre start WorkerActor %s".format(name))

      socket.setIdentity(name + "_b")
      socket.connect("tcp://localhost:5566")

      self.receiveTimeout = Some(2000L)
    }

    def receive = {
      case Submit(job) => log.debug("submitting to execute %s".format(job)); executeJob(job)
      case Finish() => log.debug("sending back ready"); send(socket, "READY")
      case ReceiveTimeout => log.debug("ww %s inner control timed out".format(name))
    }

    override def postStop() = {
      log.warning("Stopping WorkerActor %s".format(name))
    }

  }

  val worker = actorOf(new WorkerActor()).start()

  def send(msg: String): Unit = send(socket, msg)

  def send(socket: ZMQ.Socket, msg: String) = {
    log.debug("SENDING %s from %s".format(msg, name));
    sendParts(socket, name, "", msg)
  }

}

object ZeromqTest extends App {
  val pc = new PirateClient()

  def startWorkers() = {
    for (i <- 1 to 3)
      new PirateWorker("w" + i)
    //    Thread.sleep(1000)
    //    val pw2 = new PirateWorker("w2")
  }

  startWorkers()

  Thread.sleep(4000)
  println("SENDING COMMAND storm")

  for (i <- 1 to 100) {
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
