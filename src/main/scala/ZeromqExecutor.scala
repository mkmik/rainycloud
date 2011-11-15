package it.cnr.aquamaps
import com.google.gson.Gson
import net.lag.configgy.Configgy

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

import com.weiglewilczek.slf4s.Logging
import cloud.TaskLauncher

class ZeromqTaskExecutor(val name: String) extends ZeromqHandler with ZeromqJobSubmitterExecutorCommon with Logging {
  import Zeromq._

  case class Submit(val task: TaskRef)

  val worker = actorOf(new WorkerActor()).start()

  val socket = context.socket(ZMQ.XREQ)

  new Thread(() => {

    socket.setIdentity(name)
    socket.bind("inproc://executor_%s".format(name))
    socket.connect("tcp://%s:%s".format(Configgy.config.getString("queue-host").getOrElse("localhost"),Configgy.config.getInt("queue-port").getOrElse(5566)))

    logger.info("registering %s".format(name))
    send("READY")

    val poller = context.poller()
    poller.register(socket, ZMQ.Poller.POLLIN)

    def eventLoop(): Unit = {
      while (true) {
        val res = poller.poll(1000 * 1000)
        if (res > 0) {

          logger.debug("W %s got poll in".format(name))

          val address = getAddress()
          recv() match {
            case "KILL" =>
              logger.info("W %s was shot in the head, dying".format(name))
              return
            case "SUBMIT" =>
              val task = recv()
              logger.debug("W %s got submission '%s'".format(name, task))
              worker ! Submit(TaskRef(task))
              logger.debug("worker actor messaged")
          }

        }

        send("HEARTBEAT")
      }
    }

    eventLoop()
    logger.warn("W %s died".format(name))
  }).start()

  var taskRunning = false

  def executeTask(task: TaskRef): Unit = {
    taskRunning = true

    logger.info("W %s will spawn background task".format(name))
    spawn {
      logger.debug("W %s is working for real (mumble mumble)".format(name))
      //        logger.debug("W %s is working on step %s of task %s".format(name, i, task))
      TaskLauncher.launch(task, worker)
//      for(i <- 1 to 40) {
//        Thread.sleep(100)
//        worker ! Progress(task, 551, 100)
//      }
      //}
      logger.info("W %s finished computing task %s".format(name, task))
      worker ! Finish(task)
    }
  }

  class WorkerActor extends Actor with Logging {

    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self, 15, 100.milliseconds)

    val innerSocket = context.socket(ZMQ.XREQ)

    override def preStart() = {
      logger.debug("pre start WorkerActor %s".format(name))

      innerSocket.setIdentity(name + "_b")
      innerSocket.connect("tcp://localhost:5566")

      self.receiveTimeout = Some(2000L)
    }

    /*# It could happen that we sent our "ready" but no server was listening,
     * we can resend the "READY" message once in a while, just make sure we are not
     * executing something right now. */
    def perhapsRecover() = {
      if (!taskRunning)
        send(innerSocket, "READY")
    }

    def finished(task: TaskRef) = {
      taskRunning = false

      send(innerSocket, "SUCCESS", task.id)
      send(innerSocket, "READY")
    }

    val gson = new Gson

    def trackProgress(progress: Progress) = {
      send(innerSocket, "PROGRESS", gson.toJson(progress))
    }

    def receive = {
      case Submit(task) => logger.debug("submitting to execute %s".format(task)); executeTask(task)
      case progress: Progress => trackProgress(progress) 
      case Finish(task) => logger.debug("sending back ready"); finished(task);
      case ReceiveTimeout => /* logger.debug("ww %s inner control timed out".format(name)); */ perhapsRecover()
    }

    override def postStop() = {
      logger.warn("Stopping WorkerActor %s".format(name))
    }

  }

  def send(msg: String): Unit = send(socket, msg)

  def send(socket: ZMQ.Socket, msg: String) = {
    //logger.debug("SENDING %s from %s".format(msg, name));
    sendParts(socket, name, "", msg)
  }

  // TODO: cleanup
  def send(socket: ZMQ.Socket, msg: String, arg: String) = {
    //logger.debug("SENDING %s from %s".format(msg, name));
    sendParts(socket, name, "", msg, arg)
  }

}

case class TaskRef(val id: String)
case class Progress(val task: TaskRef, amount: Long, delta: Long)
