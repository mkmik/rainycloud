package it.cnr.aquamaps.cloud

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.Dispatchers
import java.util.concurrent._
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
import com.google.gson.Gson
import java.util.UUID


class EmbeddedJobSubmitter extends JobSubmitter with Logging {
  def workers = Map()
  def queueLength = 0
  def newJob(jobRequest: JobRequest) = {
    new EmbeddedJob(jobRequest)
  }
}

object EmbeddedJob {
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
}

class EmbeddedJob (val jobRequest: JobRequest) extends JobSubmitter.Job with Logging {
  import EmbeddedJob._

  val id = UUID.randomUUID.toString

  def completed = runningJob match {
    case Some(f) => f.isCompleted
    case None => false
  }

  def totalTasks = 0
  def completedTasks = 0

  def addTask(spec: JobSubmitter.TaskSpec) {}

  var runningJob: Option[Future[Unit]] = None

  def seal {
    runningJob = Some(Future {(new EmbeddedExecutor).run})
  }

  class EmbeddedExecutor {
    def run {
      println("LAUNCHING JOB REQUEST %s".format(jobRequest))

      val injector = Guice createInjector (GuiceModules `override` AquamapsModule() `with` (EmbeddedJobModule(jobRequest), HDFSModule()))
      val entryPoint = injector.instance[EntryPoint]
      entryPoint.run

      cleanup(injector)
    }

    def cleanup(injector: Injector) {
      /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
      logger.info("---- job done")
      injector.instance[Fetcher[HCAF]].shutdown
      injector.instance[Loader[HSPEN]].shutdown
    }
  }

}

case class EmbeddedJobModule(val jobRequest: JobRequest) extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    // doesn't override
    //    bind[TableWriter[HSPEC]].toInstance(new FileSystemTableWriter(conf.getString("hspecFile").getOrElse("/tmp/hspec.csv.gz")))
    //        bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]].in[Singleton]

    bind[JobRequest].toInstance(jobRequest)
    bind[Emitter[HSPEC]].to[CopyDatabaseHSPECEmitter].in[Singleton]

  }
}
