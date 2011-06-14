package it.cnr.aquamaps.cloud

import it.cnr.aquamaps._

import net.lag.logging.Logger
import net.lag.configgy.{ Config, Configgy }

object Submitter extends App {
  private val log = Logger(Submitter getClass)

  val js = new ZeromqJobSubmitter()

  if (!Configgy.config.getBool("web").getOrElse(false)) {
    Thread.sleep(4000)
    log.info("SENDING COMMAND storm")

    runTest()
  }

  def runTest() {
            
    val job = js.newJob()
    for (i <- 1 to 40)
      job.addTask(js.newTaskSpec("wow" + i))
    job.seal()
      
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
