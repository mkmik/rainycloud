package it.cnr.aquamaps.cloud
import akka.actor.ActorRef
import com.google.gson.Gson
import it.cnr.aquamaps._

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }

import net.lag.logging.Logger

case class LauncherModule(val jobRequest: JobRequest, val jobSubmitter: JobSubmitter) extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
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
    def job(jobSubmitter: JobSubmitter) = jobSubmitter.newJob
  }
}

class DummyLoader[A] extends Loader[A] {
  def load: Iterable[A] = List()
}

case class TaskRequest(val partition: Partition, val job: JobRequest)

class CloudGenerator @Inject() (val jobRequest: JobRequest, val job: JobSubmitter.Job, val submitter: Submitter) extends Generator {
  private val log = Logger(classOf[CloudGenerator])

  val gson = new Gson()

  def computeInPartition(p: Partition) = {
    log.info("Cloud generator computing in partition %s".format(p))

    job.addTask(submitter.js.newTaskSpec(gson.toJson(TaskRequest(p, jobRequest))))
  }
}

class CloudHSPECEmitter @Inject() (val job: JobSubmitter.Job, val submitter: Submitter) extends Emitter[HSPEC] {
  def emit(record: HSPEC) = {} // dummy

  def flush = {
    job.seal()
    submitter.registerJob(job)
  }
}

/** a Launcher prepares the environment for the entry point so that it can use Submitter */
class Launcher @Inject() (val jobSubmitter: JobSubmitter) {
  private val log = Logger(classOf[Launcher])

  def launch(jobRequest: JobRequest) = {
    val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (LauncherModule(jobRequest, jobSubmitter), HDFSModule()))

    val entryPoint = injector.instance[EntryPoint]
    entryPoint.run

    cleanup(injector)
  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    log.info("done")
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


    bind[Emitter[HSPEC]].to[DatabaseHSPECEmitter]
    bind[Partitioner].toInstance(new StaticPartitioner(Seq("%s %s".format(taskRequest.partition.start, taskRequest.partition.size)).toIterator))
  }
}

class DatabaseHSPECEmitter extends Emitter[HSPEC] {
  def emit(record: HSPEC) = {}
  def flush = {}
}


object TaskLauncher {
  private val log = Logger(TaskLauncher getClass)

  val gson = new Gson()

  def launch(task: TaskRef, worker: ActorRef) = {
    println("launching task %s".format(task.id))

    val taskRequest = gson.fromJson(task.id, classOf[TaskRequest])

    val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (TaskLauncherModule(taskRequest), HDFSModule()))
    val entryPoint = injector.instance[EntryPoint]
    entryPoint.run

    cleanup(injector)
    
  }

  def cleanup(injector: Injector) {
    /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
    log.info("done")
    injector.instance[Fetcher[HCAF]].shutdown
    injector.instance[Loader[HSPEN]].shutdown
  }

}
