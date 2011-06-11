package it.cnr.aquamaps

import org.zeromq.ZMQ
import scala.actors.Futures._

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef
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

trait JobSubmitterCommon {
  
}

trait ZeromqJobSubmitterExecutorCommon {
  case class TaskRef(val id: String)
  case class Finish(val task: TaskRef)
}

trait ZeromqJobSubmitterCommon extends JobSubmitterCommon {
  case class Task(val id: String) {
    override def toString = id
  }

  case class Worker(val name: String) {
    var currentTask: Option[Task] = None
    override def toString = name
  }


  /*# Command events */
  case class Submit(val task: Task)
  /*# Status feedback events */
  case class Ready(val worker: Worker)
  case class Success(val taskId: String, val worker: Worker)
  //  case class Rejected(val task: Task)
  case class Died(val worker: Worker)
  // a node joins when it sends active heartbeat
  case class Joined(val worker: Worker)
  /*# Commands/actions */
  case class Kill(val worker: String)

}

trait JobSubmitter {

  case class TaskSpec(val spec: String)

  trait Job {
    def addTask(spec: TaskSpec)
    def waitCompletion() = {}
  }

  def newJob(): Job
  def newTaskSpec(spec: String) = TaskSpec(spec)
}

class ZeromqJobSubmitter extends ZeromqHandler with JobSubmitter with ZeromqJobSubmitterCommon {
  import Zeromq._

  private val log = Logger(classOf[ZeromqJobSubmitter])

  class ZeromqJob extends Job {
    class JobActor extends Actor {
      var tasks = List[TaskSpec]()

      private val log = Logger(classOf[JobActor])

      self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, 15, 100.milliseconds)
      def receive = {
        case spec: TaskSpec =>
          tasks = spec +: tasks
          submitTask(Task(spec.spec))
      }
    }
    val runningJob = actorOf(new JobActor()).start

    def addTask(spec: TaskSpec) = {
      runningJob ! spec
    }
  }

  def newJob() = new ZeromqJob()

  val socket = context.socket(ZMQ.XREP)
  socket.bind("inproc://client")
  socket.bind("tcp://*:5566")

  /*# This "actor" implements the communication between the submission client and the workers.
   It handles worker registration, heartbeating, task book keeping. Higher level task handling is
   done in the 'sender' actor. */
  spawn {
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

    def submitTask(worker: Worker, task: Task) {
      sendParts(worker.name, socket.getIdentity(), "", "SUBMIT", task.toString)
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
          case "SUBMIT" => submitTask(worker, Task(recv()))
          case "SUCCESS" => sender ! Success(recv(), worker)
          case _ => throw new IllegalArgumentException("unknown command '%s' for worker '%s' in thread %s".format(msg, worker, Thread.currentThread()))
        }
      }

      checkDeaths()
    }
  }

  //

  /*# This actor implements the API between the submission API users and the zmq subsystem.
   Furthermore it handles the task rejection and retries. */
  // {{{
  class SenderActor extends Actor {
    private val log = Logger(classOf[SenderActor])

    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, 15, 100.milliseconds)

    val socket = context.socket(ZMQ.XREQ)
    socket.connect("inproc://client")

    var ongoingTasks = Map[String, Task]()
    var queuedTasks = Queue[Task]()
    var readyWorkers = Queue[Worker]()

    // statistical
    var completedTasks = 0

    def taskCompleted(taskId: String, worker: Worker): Unit = {
      ongoingTasks.get(taskId) match {
        case Some(task) => taskCompleted(task, worker)
        case None => log.warning("task %s completed but I don't know anything about this task, ignoring".format(taskId))
      }
    }

    def taskCompleted(task: Task, worker: Worker) = {
      log.info("completing task %s".format(task.id))
      ongoingTasks = ongoingTasks - task.id
      completedTasks += 1
    }

    def submit(task: Task): Unit = {
      queuedTasks = queuedTasks enqueue task
      flushQueue()
    }

    def flushQueue() = {
      val slots = readyWorkers zip queuedTasks
      log.debug("FLUSHING QUEUE ready workers: %s, queuedTasks: %s, matched slot size: %s".format(readyWorkers.size, queuedTasks.size, slots.size))
      for ((worker, task) <- slots) {
        log.debug("can run %s on %s".format(task, worker))
        assignTask(task, worker)
      }

      queuedTasks = queuedTasks drop (slots.size)

      log.debug("ready workers was: %s".format(readyWorkers))
      readyWorkers = readyWorkers drop (slots.size)
      log.debug("ready workers is:  %s".format(readyWorkers))
    }

    def assignTask(task: Task, worker: Worker): Unit = {
      ongoingTasks += ((task.id, task))
      assert(worker.currentTask == None)
      worker.currentTask = Some(task)
      sendToWorker(worker, task)
    }

    def sendToWorker(worker: Worker, task: Task): Unit = sendParts(socket, worker.name, "", "SUBMIT", task.toString)

    def acceptWorker(worker: Worker) = {
      log.debug("Worker %s is now ready.Previous ready workers: %s (%s)".format(worker, readyWorkers, Thread.currentThread()))
      readyWorkers = readyWorkers enqueue worker
      log.debug("                        Current ready workers: %s".format(readyWorkers))
      flushQueue()
    }

    def buryWorker(worker: Worker) = {
      log.warning("worker '%s' is dead while running".format(worker))
      worker.currentTask match {
        case Some(task) =>
          log.warning("WWWWWWWWWWWWWWW worker %s died while running task %s, resubmitting".format(worker, task))
          submit(task)
        case None => log.info("worker %s died but it wasn't running any task, goodbye".format(worker))
      }
      readyWorkers = readyWorkers.filterNot(el => el.name == worker.name)
    }

    def summary() = {
      log.info(" -------> Currently %s workers alive (%s), queue length: %s, completed tasks %d".format(readyWorkers.length, readyWorkers, queuedTasks.length, completedTasks))
    }

    self.receiveTimeout = Some(4000L)

    def receive = {
      case Ready(worker) => acceptWorker(worker)
      case Success(taskId, worker) => taskCompleted(taskId, worker)
      case Joined(worker) => log.info("Worker %s has joined".format(worker))
      case Died(worker) => buryWorker(worker)
      case Kill(worker) => sendParts(socket, "dummy", "", "KILL", worker)
      case Submit(task) => submit(task)
      case ReceiveTimeout => summary()
    }
  }
  // }}}

  val sender = actorOf(new SenderActor()).start

  def submitTask(task: Task) = sender ! Submit(task)
  def kill(worker: String) = sender ! Kill(worker)
}

class ZeromqTaskExecutor(val name: String) extends ZeromqHandler with ZeromqJobSubmitterExecutorCommon {
  import Zeromq._

  private val log = Logger(classOf[ZeromqTaskExecutor])

  case class Submit(val task: TaskRef)

  val socket = context.socket(ZMQ.XREQ)

  new Thread(() => {
    socket.setIdentity(name)
    socket.connect("tcp://localhost:5566")

    log.info("sending ready from %s".format(name))
    send("READY")
    log.info("sent ready from %s".format(name))

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
              val task = recv()
              log.debug("W %s got submission '%s'".format(name, task))
              worker ! Submit(TaskRef(task))
              log.debug("worker actor messaged")
          }

        }

        send("HEARTBEAT")
      }
    }

    eventLoop()
    log.warning("W %s died".format(name))
  }).start()

  def executeTask(task: TaskRef): Unit = {
    log.info("W %s will spawn background task for (about 6 sec)".format(name))
    spawn {
      log.debug("W %s is working for real (about 6 sec)".format(name))
      for (i <- 1 to 6) {
        log.debug("W %s is working on step %s of task %s".format(name, i, task))
        Thread.sleep(100)
      }
      log.info("W %s finished computing task %s".format(name, task))
      worker ! Finish(task)
    }
  }

  class WorkerActor extends Actor {
    private val log = Logger(classOf[WorkerActor])

    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, 15, 100.milliseconds)

    val socket = context.socket(ZMQ.XREQ)

    override def preStart() = {
      log.debug("pre start WorkerActor %s".format(name))

      socket.setIdentity(name + "_b")
      socket.connect("tcp://localhost:5566")

      self.receiveTimeout = Some(2000L)
    }

    def receive = {
      case Submit(task) => log.debug("submitting to execute %s".format(task)); executeTask(task)
      case Finish(task) => log.debug("sending back ready"); send(socket, "SUCCESS", task.id); send(socket, "READY")
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

  // TODO: cleanup
  def send(socket: ZMQ.Socket, msg: String, arg: String) = {
    log.debug("SENDING %s from %s".format(msg, name));
    sendParts(socket, name, "", msg, arg)
  }

}

object ZeromqTest extends App {
  val pc = new ZeromqJobSubmitter()

  def startWorkers() = {
    for (i <- 1 to 20) {
      val worker = new ZeromqTaskExecutor("w" + i)
      Thread.sleep(100)
      println("Is it running %s ? %s".format("w" + i, worker.worker.isRunning))
    }
    //    val pw2 = new PirateWorker("w2")
  }

  startWorkers()

  Thread.sleep(4000)
  println("SENDING COMMAND storm")

  val job = pc.newJob()
  for(i <- 1 to 20)
    job.addTask(pc.newTaskSpec("wow" + i))

  /*
  for (i <- 1 to 100) {
    pc.submitTask(JobSubmitterCommon#Task("Test Command %s".format(i)))
  }
*/

  Thread.sleep(3000)
  for (i <- 1 to 19) {
    pc.kill("w" + i)
  }

}
