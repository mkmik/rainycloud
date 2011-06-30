package it.cnr.aquamaps.cloud
import it.cnr.aquamaps._

import javax.servlet.Servlet

import org.scalatra._
import org.scalatra.scalate._
import org.scalatra.socketio.SocketIOSupport
import com.glines.socketio.server.SocketIOFrame

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{ Text, Node }

import net.lag.logging.Logger
import net.lag.configgy.{ Config, Configgy }

class SubmitterApi extends ScalatraServlet with ScalateSupport with UrlSupport {
  import JobSubmitter.Job

  val log = Logger(classOf[SubmitterApi])

  beforeAll {
    contentType = "application/json"
  }

  override def contextPath = "/api"

  val style = """ """

  post("/submit") {
    log.info("posted")

    val job = Submitter.spawnTest()
    val id = job.id

    """{"error": null,"id": "%s"}""".format(id)

  }

  get("/status/:id") {
    val id = params("id")
    val job = Submitter.jobs().get(id)
    job match {
      case None =>
        """{"error" : "unknown job"}"""
      case Some(job) =>
        val status = if (job.completed) "DONE" else "RUNNING"
        val metrics = if (job.completed) """""""" else buildMetrics(job)
        val completion = (job.completedTasks: Double) * 100.0 / job.totalTasks

        """{"id":"%s","status":"%s","completion":%s,"metrics":%s}""".format(id, status, completion, metrics)
    }
  }

  def buildMetrics(job: Job) = {
    """ {"load":[{"resId":"W0","value":82.374146}],"throughput":[1308157425388,2722647]} """"
  }

}

