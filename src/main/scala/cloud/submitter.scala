package it.cnr.aquamaps

import it.cnr.aquamaps.cloud.JobRequest


case class TaskRef(val id: String)
case class Progress(val task: TaskRef, amount: Long, delta: Long)


trait JobSubmitter {
  import JobSubmitter._

  def newJob(jobRequest: JobRequest): Job
  def newTaskSpec(spec: String) = TaskSpec(spec)

  // for monitoring
  def queueLength: Int
  def workers: Map[String, JobSubmitter.WorkerDescriptor]
}

object JobSubmitter {
  case class TaskSpec(val spec: String)
  case class WorkerDescriptor(val completed: Int, val heartbeatAgo: Long, val uptime: Long)

  trait Job {
    def addTask(spec: TaskSpec)
    def waitCompletion() = {}
    /*# Don't allow addition of any new tasks */
    def seal()

    def startTime = System.currentTimeMillis
    def totalTasks: Int
    def completedTasks: Int
    def completed: Boolean
    def error: Option[String] = None

    val id: String
  }
}
