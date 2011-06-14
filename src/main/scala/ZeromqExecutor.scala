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

class ZeromqTaskExecutor(val name: String) extends ZeromqHandler with ZeromqJobSubmitterExecutorCommon {
  import Zeromq._

  private val log = Logger(classOf[ZeromqTaskExecutor])

  case class Submit(val task: TaskRef)

  val socket = context.socket(ZMQ.XREQ)

  val worker = actorOf(new WorkerActor()).start()

  new Thread(() => {
    socket.setIdentity(name)
    socket.connect("tcp://localhost:5566")

    log.info("registering %s".format(name))
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

  var taskRunning = false

  def executeTask(task: TaskRef): Unit = {
    taskRunning = true

    log.info("W %s will spawn background task".format(name))
    spawn {
      log.debug("W %s is working for real (mumble mumble)".format(name))
      //        log.debug("W %s is working on step %s of task %s".format(name, i, task))
        Thread.sleep(10)
      //}
      log.info("W %s finished computing task %s".format(name, task))
      worker ! Finish(task)
    }
  }

  def finished(task: TaskRef) = {
    taskRunning = false

    send(socket, "SUCCESS", task.id)
    send(socket, "READY")
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

    /*# It could happen that we sent our "ready" but no server was listening,
     * we can resend the "READY" message once in a while, just make sure we are not
     * executing something right now. */
    def perhapsRecover() = {
      if (!taskRunning)
        send(socket, "READY")
    }

    def receive = {
      case Submit(task) => log.debug("submitting to execute %s".format(task)); executeTask(task)
      case Finish(task) => log.debug("sending back ready"); finished(task);
      case ReceiveTimeout => log.debug("ww %s inner control timed out".format(name)); perhapsRecover()
    }

    override def postStop() = {
      log.warning("Stopping WorkerActor %s".format(name))
    }

  }

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

