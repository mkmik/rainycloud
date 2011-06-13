package it.cnr.aquamaps.cloud

import it.cnr.aquamaps._


object Submitter extends App {
  val pc = new ZeromqJobSubmitter()

  def startWorkers() = {
    for (i <- 1 to 20) {
      val worker = new ZeromqTaskExecutor("w" + i)
      Thread.sleep(100)
      println("Is it running %s ? %s".format("w" + i, worker.worker.isRunning))
    }
    //    val pw2 = new PirateWorker("w2")
  }

//  startWorkers()

  Thread.sleep(4000)
  println("SENDING COMMAND storm")

  val job = pc.newJob()
  for (i <- 1 to 20)
    job.addTask(pc.newTaskSpec("wow" + i))
  job.seal()

  Thread.sleep(1000)
  println(">>>>>>>>>>>>>>>>>>>> Polling for status Checking total tasks")
  println(">>>>>>>>>>>>>>>>>>>> Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
  while (!job.completed) {
    Thread.sleep(1000)
    println("Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))
  }
  println(">>>>>>>>>>>>>>>>>>>> Total job tasks %s, completed tasks %s. Completed ? %s".format(job.totalTasks, job.completedTasks, job.completed))

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
