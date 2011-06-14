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

