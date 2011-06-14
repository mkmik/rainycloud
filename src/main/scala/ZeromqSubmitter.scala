package it.cnr.aquamaps

import org.zeromq.ZMQ

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

    var workers = new HashMap[WorkerRef, Long]()

    def workerReady(worker: WorkerRef) = {
      workerAlive(worker)

      sender ! Ready(worker)
    }

    def workerAlive(worker: WorkerRef) = {
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

    def submitTask(worker: WorkerRef, task: TaskRef) {
      sendParts(worker.name, socket.getIdentity(), "", "SUBMIT", task.id)
    }

    while (true) {
      val events = poller.poll(500 * 1000)
      if (events > 0) {
        val worker = WorkerRef(getAddress())
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
    var workers = Map[String, Worker]()

    // statistical
    var completedTasks = 0

    def taskCompleted(taskId: String, worker: WorkerRef): Unit = {
      ongoingTasks.get(taskId) match {
        case Some(task) => taskCompleted(task, worker)
        case None => log.warning("task %s completed but I don't know anything about this task, ignoring".format(taskId))
      }
    }

    def taskCompleted(task: Task, worker: WorkerRef) = {
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
      log.debug("ASSIGNED TASK %s to worker %s".format(task, worker))
      sendToWorker(worker, task)
    }

    def sendToWorker(worker: Worker, task: Task): Unit = sendParts(socket, worker.name, "", "SUBMIT", task.id)

    def acceptWorker(workerRef: WorkerRef): Unit = {
      workers.get(workerRef.name) match {
        case None =>
          log.warning("accepting new worker")
          val worker = Worker(workerRef.name)
          workers += ((workerRef.name, worker))
          acceptWorker(worker)
        case Some(worker) => acceptWorker(worker)
      }
    }

    def acceptWorker(worker: Worker): Unit = {
      // we are here because the worker signaled that it's free accepting new tasks
      worker.currentTask = None

      log.debug("Worker %s is now ready.Previous ready workers: %s (%s)".format(worker, readyWorkers, Thread.currentThread()))
      if (!(readyWorkers contains worker)) {
        readyWorkers = readyWorkers enqueue worker
        log.debug("                        Current ready workers: %s".format(readyWorkers))
        flushQueue()
      }
    }

    def buryWorker(workerRef: WorkerRef): Unit = {
      workers.get(workerRef.name) match {
        case None => log.warning("an unknown worker died: %s".format(workerRef))
        case Some(worker) => buryWorker(worker)
      }

    }

    def buryWorker(worker: Worker): Unit = {
      log.warning("worker '%s' is dead while running".format(worker))
      worker.currentTask match {
        case Some(task) =>
          log.warning("WWWWWWWWWWWWWWW worker %s died while running task %s, resubmitting".format(worker, task))
          submit(task)
        case None => log.info("XXXXXXXXXXXXX worker %s died but it wasn't running any task, goodbye".format(worker))
      }
      readyWorkers = readyWorkers.filterNot(el => el.name == worker.name)
      workers -= worker.name
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

