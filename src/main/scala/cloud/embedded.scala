package it.cnr.aquamaps.cloud

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.agent.Agent
import akka.dispatch.Await
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
  implicit val system = ActorSystem("app")
}

class EmbeddedJob (val jobRequest: JobRequest) extends JobSubmitter.Job with Logging {
  import EmbeddedJob._

  val id = UUID.randomUUID.toString

  def completed = runningJob match {
    case Some(f) => f.isCompleted
    case None => false
  }

  var totalTasksGetter = () => 2
  var completedTasksGetter = () => 0

  def totalTasks = totalTasksGetter()
  def completedTasks = completedTasksGetter()
  override def error = errorString

  var errorString: Option[String] = None

  def addTask(spec: JobSubmitter.TaskSpec) {}

  var runningJob: Option[Future[Unit]] = None

  def seal {
    runningJob = Some(Future {(new EmbeddedExecutor).run})
  }

  class EmbeddedExecutor extends Logging {
    def run {
      println("LAUNCHING JOB REQUEST %s".format(jobRequest))

      try {
        val injector = Guice createInjector (GuiceModules `override` (GuiceModules `override` new AquamapsModule() `with` HDFSModule()) `with` EmbeddedJobModule(jobRequest))
        val entryPoint = injector.instance[EntryPoint]

        val emitter = injector.instance[CountingEmitter[HSPEC]]
        println("GOT HSPEC EMITTER %s".format(emitter))

        totalTasksGetter = () => 158711108
        completedTasksGetter = () => emitter.count

        try {
          entryPoint.run
        } finally {
          println("TOTAL NUMBER OF ELEMENTS", emitter.count)
          cleanup(injector)
          completedTasksGetter = () => -1
        }
      } catch {
        case e: Throwable =>
          logger.error("Got exceptions while running", e)
          errorString = Some(e.getMessage)
      }
    }

    def cleanup(injector: Injector) {
      /*! currently Guice lifecycle support is lacking, so we have to perform some cleanup */
      logger.info("---- job done")
      injector.instance[Fetcher[HCAF]].shutdown
      injector.instance[Loader[HSPEN]].shutdown
    }
  }

}

class ParallelEmitter[A](val downstream: Emitter[A]) extends Emitter[A] {
  import EmbeddedJob._

  val partitions = Agent(List[Future[Unit]]())

  def emit(record: A) = downstream.emit(record)

  def flush = {
    Await.result(Future.sequence(partitions.get), 12 hours)
    println("AWAITED ALL FUTURES")
    downstream.flush
  }
}

class ParallelGenerator(val downstream:Generator, val emitter: ParallelEmitter[HSPEC]) extends Generator {
  import EmbeddedJob._

  def computeInPartition(p: Partition) = {
    val fut = Future { downstream.computeInPartition(p) }
    emitter.partitions send (_ :+ fut)
  }
}

//case class EmbeddedJobModule(val jobRequest: JobRequest) extends AquamapsModule {
case class EmbeddedJobModule(val jobRequest: JobRequest) extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    bind[JobRequest].toInstance(jobRequest)

    val hspenQuery = """SELECT speciesid||':'||lifestage, Layer, SpeciesID, FAOAreas, Pelagic,
    NMostLat, SMostLat, WMostLong, EMostLong, DepthMin, DepthMax, DepthPrefMin, DepthPrefMax,
    TempMin, TempMax, TempPrefMin, TempPrefMax, SalinityMin, SalinityMax, SalinityPrefMin, SalinityPrefMax,
    PrimProdMin, PrimProdMax, PrimProdPrefMin, PrimProdPrefMax, IceConMin, IceConMax, IceConPrefMin, IceConPrefMax,
    LandDistMin, LandDistMax, LandDistPrefMin, MeanDepth, LandDistPrefMax, LandDistYN FROM %s""".format(jobRequest.hspenTableName.tableName)

    //bind[TableReader[HSPEN]].toInstance(new DBTableReader(jobRequest.hspenTableName, Some(hspenQuery)))
    //bind[Emitter[HSPEC]].to[CountingEmitter[HSPEC]].in[Singleton]
    bind[Emitter[HSPEC]].to[ParallelEmitter[HSPEC]].in[Singleton]

    val hcafQuery = """SELECT s.CsquareCode,s.OceanArea,s.CenterLat,s.CenterLong,d.FAOAreaM,DepthMin,DepthMax,
    SSTAnMean,SBTAnMean,SalinityMean, SalinityBMean,PrimProdMean,IceConAnn,d.LandDist,
    s.EEZFirst,s.LME,d.DepthMean
    FROM HCAF_S as s INNER JOIN %s as d ON s.CSquareCode=d.CSquareCode where d.oceanarea > 0""".format(jobRequest.hcafTableName.tableName)

    //bind[TableReader[HCAF]].toInstance(new DBTableReader(jobRequest.hcafTableName, Some(hcafQuery)))

    bind[Generator].to[ParallelGenerator]
  }

  @Provides
  @Singleton
  def hspecEmitter(jobRequest: JobRequest, csvSerializer: CSVSerializer): CountingEmitter[HSPEC] = new CountingEmitter(new CopyDatabaseHSPECEmitter(jobRequest, csvSerializer))
  //def hspecEmitter(jobRequest: JobRequest, csvSerializer: CSVSerializer): CountingEmitter[HSPEC] = new CountingEmitter(new CSVEmitter(new CSVPositionalSink(new FileSystemTableWriter("/tmp/my-hspec-%s.gz".format(math.abs(scala.util.Random.nextLong)))), csvSerializer))

  @Provides
  @Singleton
  def parallelEmitter(emitter: CountingEmitter[HSPEC]) = new ParallelEmitter(emitter)

  @Provides
  @Singleton
  def parallelGenerator(generator: HSPECGenerator, emitter: ParallelEmitter[HSPEC]) = new ParallelGenerator(generator, emitter)
}
