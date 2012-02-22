package it.cnr.aquamaps

import com.weiglewilczek.slf4s.Logging


class DummyJobSubmitter extends JobSubmitter with Logging {
  def workers = Map()
  def queueLength = 0
  def newJob = new DummyJob
}

class DummyJob extends JobSubmitter.Job {
  val id = ""
  def completed = false
  def totalTasks = 0
  def completedTasks = 0
  def seal {}
  def addTask(spec: JobSubmitter.TaskSpec) {}
}
