package it.cnr.aquamaps.cloud

import it.cnr.aquamaps._

import net.lag.logging.Logger
import net.lag.configgy.{ Config, Configgy }

import akka.agent.Agent
import java.util.UUID

object Submitter extends App {
  private val log = Logger(Submitter getClass)

  val js = new ZeromqJobSubmitter()

  if (!Configgy.config.getBool("web").getOrElse(false)) {
    Thread.sleep(4000)
    log.info("SENDING COMMAND storm")

    runTest()
  }

  val jobs = Agent(Map[String, JobSubmitter.Job]())
  val workers = Agent(Map[String, JobSubmitter.Worker]())

  def queueLength = js.queueLength
  
  def registerJob(job: JobSubmitter.Job) = {
    val uuid = UUID.randomUUID.toString
    jobs send (_ + ((uuid, job)))
    uuid
  }

  def deleteJob(id: String) = {
    jobs send (_ - id)
  }

  def killJob(id: String) = {
//    jobs send (_ - id)
  }


  def spawnTest() = {
    val job = js.newJob()
    for (i <- 1 to 10)
      job.addTask(js.newTaskSpec("wow" + i))
    job.seal()
    registerJob(job)
    job
  }

  def runTest() {
    val job = spawnTest()
      
    Thread.sleep(1000)
    log.info(">>>>>>>>>>>>>>>>>>>> Polling for status Checking total tasks")
    log.info(">>>>>>>>>>>>>>>>>>>> Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
    while (!job.completed) {
      Thread.sleep(1000)
      log.info("Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
    }
    log.info(">>>>>>>>>>>>>>>>>>>> Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
        
  }

}
