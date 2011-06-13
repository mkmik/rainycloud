package it.cnr.aquamaps

import org.zeromq.ZMQ
import scala.actors.Futures._

import akka.agent.Agent
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
  case class Task(val id: String, listener: ActorRef) {
    override def toString = id
  }

  case class Worker(val name: String) {
    var currentTask: Option[Task] = None
    override def toString = name
  }

  /*# Command events */
  case class Submit(val task: Task)
  /*# Status feedback events */
  case class Completed(val task: Task)
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
    /*# Don't allow addition of any new tasks */
    def seal()

    def totalTasks: Int
    def completedTasks: Int
    def completed: Boolean
  }

  def newJob(): Job
  def newTaskSpec(spec: String) = TaskSpec(spec)

}

class ZeromqJobSubmitter extends ZeromqHandler with JobSubmitter with ZeromqJobSubmitterCommon with ZeromqJobSubmitterExecutorCommon {
  import Zeromq._

  private val log = Logger(classOf[ZeromqJobSubmitter])

  class ZeromqJob extends Job {
    class JobActor extends Actor {
      var tasks = List[TaskSpec]()

      private val log = Logger(classOf[JobActor])

      def receive = {
        case spec: TaskSpec =>
          tasks = spec +: tasks
          submitTask(Task(spec.spec, self))
          totalTasksAgent send (_ + 1)
        case Completed(task) =>
          completedTasksAgent send (_ + 1)
          completedTasksAgent.await()
          totalTasksAgent.await()
          if (isSealed && completedTasks >= totalTasks)
            completedAgent send true
      }
    }
    val runningJob = actorOf(new JobActor()).start

    var totalTasksAgent = Agent(0)
    var completedTasksAgent = Agent(0)

    var completedAgent = Agent(false)
    val sealedAgent = Agent(false)

    def addTask(spec: TaskSpec) = {
      runningJob ! spec
    }

    def totalTasks = totalTasksAgent()
    def completedTasks = completedTasksAgent()

    def seal = sealedAgent send true
    def isSealed = sealedAgent()
    def completed = completedAgent()
  }

  def newJob() = new ZeromqJob()

  val socket = context.socket(ZMQ.XREP)
  socket.bind("inproc://client")
  socket.bind("tcp://*:5566")

  val sender = actorOf(new SenderActor()).start

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

    def submitTask(worker: Worker, task: TaskRef) {
      sendParts(worker.name, socket.getIdentity(), "", "SUBMIT", task.id)
    }

    while (true) {
      val events = poller.poll(500 * 1000)
      if (events > 0) {
        val worker = Worker(getAddress())
        val msg = recv()

        msg match {
          case "READY" => workerReady(worker)
          case "HEARTBEAT" => workerAlive(worker)
          case "KILL" => sendParts(recv(), socket.getIdentity(), "", "KILL")
          case "SUBMIT" => submitTask(worker, TaskRef(recv()))
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
      task.listener ! Completed(task)
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

    def sendToWorker(worker: Worker, task: Task): Unit = sendParts(socket, worker.name, "", "SUBMIT", task.id)

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

  def submitTask(task: Task) = sender ! Submit(task)
  def kill(worker: String) = sender ! Kill(worker)
}

