package it.cnr.aquamaps

import org.scalatra._
import org.scalatra.scalate._

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }

import com.weiglewilczek.slf4s.Logging
import net.lag.configgy.{ Config, Configgy }
import javax.servlet.Servlet

object WebServer extends Logging {

  def run(servlets: Seq[(String, Servlet)], reqPort : Option[Int] = None) {
    val port = reqPort.getOrElse(Configgy.config.getInt("web-port").getOrElse(8780))

    val server = new Server(port)
    val root = new Context(server, "/", Context.SESSIONS)
    for ((route, servlet) <- servlets) {
      logger.info("adding servlet %s to %s".format(servlet, route))
      root.addServlet(new ServletHolder(servlet), route)
    }
    server.start()

    logger.info("web server started on %s".format(port))
  }
}
