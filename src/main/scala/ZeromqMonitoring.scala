package it.cnr.aquamaps

import it.cnr.aquamaps.cloud._

import org.scalatra._
import org.scalatra.scalate._

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{Text, Node}

import net.lag.logging.Logger
import net.lag.configgy.{ Config, Configgy }

class ZeromqMonitoring extends ScalatraServlet with ScalateSupport with UrlSupport {
  val log = Logger(classOf[ZeromqMonitoring])

  beforeAll {
    contentType = "text/html"
  }

  val style = """ """

  override def contextPath = "/submitter"

  def render(content: Seq[Node]) = {
    <html>
        <head>
          <title>Rainy cloud</title>
          <style>{ style }</style>
        </head>
        <body>
          <a href={url("/")}>Home</a>
          <a href={url("/submit-test")}>Submit test</a>
          <hr/>
          { content }
        </body>
      </html>
  }

  get("/") {
    render( <div>TODO: list of running jobs</div>)
  }

  get("/submit-test") {
    log.info("submitting test jobs")
    Submitter.runTest()
    redirect(url("/"))
  }


}
