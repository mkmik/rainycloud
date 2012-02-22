package it.cnr.aquamaps.cloud

import it.cnr.aquamaps._

import com.weiglewilczek.slf4s.Logging
import net.lag.configgy.{ Config, Configgy }

import akka.actor.ActorSystem
import akka.agent.Agent

import com.google.inject._
import com.google.inject.util.{ Modules => GuiceModules }
import uk.me.lings.scalaguice.InjectorExtensions._

class Submitter @Inject() (val js: JobSubmitter) extends Logging {

  def jobs = Submitter.jobs
  def workers = js.workers

  def queueLength = js.queueLength
  
  def registerJob(job: JobSubmitter.Job) = {
    jobs send (_ + ((job.id, job)))
    job.id
  }

  def deleteJob(id: String) = {
    jobs send (_ - id)
  }

  def killJob(id: String) = {
//    jobs send (_ - id)
  }


}

object Submitter {
  implicit val system = ActorSystem("app")
  val jobs = Agent(Map[String, JobSubmitter.Job]())
}

/*
object SubmitterTester extends App with Logging{

  val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` WebModule())
  val submitter = injector.instance[Submitter]

  if (!Configgy.config.getBool("web").getOrElse(false)) {
    Thread.sleep(4000)
    logger.info("SENDING COMMAND storm")

    runTest()
  }


  def spawnTest() = {
    val job = submitter.js.newJob()
    for (i <- 1 to 100)
      job.addTask(submitter.js.newTaskSpec("wow" + i))
    job.seal()
    submitter.registerJob(job)
    job
  }

  def runTest() {
    val job = spawnTest()
      
    Thread.sleep(1000)
    logger.info(">>>>>>>>>>>>>>>>>>>> Polling for status Checking total tasks")
    logger.info(">>>>>>>>>>>>>>>>>>>> Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
    while (!job.completed) {
      Thread.sleep(1000)
      logger.info("Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
    }
    logger.info(">>>>>>>>>>>>>>>>>>>> Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
        
  }

}

*/
