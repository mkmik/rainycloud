package it.cnr.aquamaps.cloud

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.Dispatchers
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import akka.util.duration._

import com.google.gson.Gson
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.querulous.evaluator.Transaction
import com.twitter.querulous.query.NullValues
import it.cnr.aquamaps._

import com.google.inject._
import it.cnr.aquamaps.jdbc.LiteDataSource
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }

import com.weiglewilczek.slf4s.Logging

case class LauncherModule(val jobRequest: JobRequest, val jobSubmitter: JobSubmitter) extends AbstractModule with ScalaModule with RainyCloudModule with Logging {
  def configure() {
    logger.info("CONF: %s".format(conf.getString("hcafFile")))

    /*! This overrides the default `Generator` to use a specific wrapper for remote submission. The `CloudGenerator` converts the parameters into serializable params
     * and spawns task using a Submitter */
    bind[Generator].to[CloudGenerator]

    bind[Emitter[HSPEC]].to[CloudHSPECEmitter]

    bind[JobSubmitter].toInstance(jobSubmitter)
    bind[JobRequest].toInstance(jobRequest)

    bind[Loader[HSPEN]].to[DummyLoader[HSPEN]]
    bind[Loader[HCAF]].to[DummyLoader[HCAF]]

    @Provides
    @Singleton
    def job(jobRequest: JobRequest, jobSubmitter: JobSubmitter) = jobSubmitter.newJob(jobRequest)
  }
}

class DummyLoader[A] extends Loader[A] {
  def load: Iterable[A] = List()
}

case class TaskRequest(val partition: Partition, val job: JobRequest)

class CloudGenerator @Inject() (val jobRequest: JobRequest, val job: JobSubmitter.Job, val submitter: Submitter) extends Generator with Logging {
  val gson = new Gson()

  def computeInPartition(p: Partition) = {
    logger.info("Cloud generator computing in partition %s".format(p))

    job.addTask(submitter.js.newTaskSpec(gson.toJson(TaskRequest(p, jobRequest))))
  }
}

class CloudHSPECEmitter @Inject() (val job: JobSubmitter.Job, val submitter: Submitter) extends Emitter[HSPEC] {
  def emit(record: HSPEC) = {} // dummy

  def flush = {
    job.seal()
    submitter.registerJob(job)
    println("<<<<<<<<<<<<<<<<<---------------------- JOB REGISTERED ? %s".format(job))
  }
}

/** a Launcher prepares the environment for the entry point so that it can use Submitter */
class Launcher @Inject() (val jobSubmitter: JobSubmitter) extends Logging {
  def launch(jobRequest: JobRequest) = {
    val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (LauncherModule(jobRequest, jobSubmitter), HDFSModule()))

    val entryPoint = injector.instance[EntryPoint]
    println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEENTRY POINT RUNNING %s".format(entryPoint))
    entryPoint.run

    val job = injector.instance[JobSubmitter.Job]
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< JOB SUBMITTER ? %s".format(jobSubmitter))

    cleanup(injector)

    job.id
  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    logger.info("done")
    injector.instance[Fetcher[HCAF]].shutdown
    injector.instance[Loader[HSPEN]].shutdown
  }
}

///

case class TaskLauncherModule(val taskRequest: TaskRequest) extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    // doesn't override
    //    bind[TableWriter[HSPEC]].toInstance(new FileSystemTableWriter(conf.getString("hspecFile").getOrElse("/tmp/hspec.csv.gz")))
    //        bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]].in[Singleton]

    bind[JobRequest].toInstance(taskRequest.job)
    bind[Emitter[HSPEC]].to[CopyDatabaseHSPECEmitter].in[Singleton]
    bind[Partitioner].toInstance(new StaticPartitioner(Seq("%s %s".format(taskRequest.partition.size, taskRequest.partition.start)).toIterator))

    bind[TaskRequest].toInstance(taskRequest)
  }
}

object TaskLauncher extends Logging {
  val gson = new Gson()

  def launch(task: TaskRef, worker: ActorRef) = {
    println("---- launching task %s".format(task.id))

    val taskRequest = gson.fromJson(task.id, classOf[TaskRequest])

    val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (TaskLauncherModule(taskRequest), HDFSModule()))
    val entryPoint = injector.instance[EntryPoint]
    entryPoint.run

    cleanup(injector)

  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    logger.info("---- task done")
    injector.instance[Fetcher[HCAF]].shutdown
    injector.instance[Loader[HSPEN]].shutdown
  }

}
