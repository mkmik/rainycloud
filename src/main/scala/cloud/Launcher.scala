package it.cnr.aquamaps.cloud
import it.cnr.aquamaps._

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.{ Modules => GuiceModules }

import net.lag.logging.Logger

case class LauncherModule(val jobSubmitter: JobSubmitter) extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    /*! This overrides the default `Generator` to use a specific wrapper for remote submission. The `CloudGenerator` converts the parameters into serializable params
     * and spawns task using a Submitter */
    bind[Generator].to[CloudGenerator]

    bind[Emitter[HSPEC]].to[CloudHSPECEmitter]

    bind[JobSubmitter].toInstance(jobSubmitter)

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

class CloudGenerator @Inject() (val job: JobSubmitter.Job, val submitter: Submitter) extends Generator {
  private val log = Logger(classOf[CloudGenerator])

  def computeInPartition(p: Partition) = {
    log.info("Cloud generator computing in partition %s".format(p))

    job.addTask(submitter.js.newTaskSpec("%s".format(p)))
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


  def launch = {
    val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (LauncherModule(jobSubmitter), HDFSModule()))

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
