package it.cnr.aquamaps

import it.cnr.aquamaps.cloud._
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.websocket.WebSocket
import org.eclipse.jetty.websocket.WebSocketServlet

import org.scalatra._
import org.scalatra.scalate._
import org.scalatra.socketio.SocketIOSupport
import com.glines.socketio.server.SocketIOFrame

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{ Text, Node }

import net.lag.logging.Logger
import net.lag.configgy.{ Config, Configgy }


class ZeromqMonitoringTest extends WebSocketServlet with Servlet {
  def doWebSocketConnect(request: HttpServletRequest,
                               protocol: String) : WebSocket = new TestWebSocket()
}

class TestWebSocket extends WebSocket {
  val log = Logger(classOf[TestWebSocket])

 def onClose (code: Int, message: java.lang.String) {
 }

 def onOpen (connection: org.eclipse.jetty.websocket.WebSocket.Connection) {
   log.info("WEB SOCKET CONNECTED")
 }

}

class ZeromqMonitoring extends ScalatraServlet with ScalateSupport with UrlSupport {
  val log = Logger(classOf[ZeromqMonitoring])

  beforeAll {
    contentType = "text/html"
  }

  val style = """ """

  override def contextPath = getServletConfig().getServletContext().getContextPath() + "/submitter"


  def render(content: Seq[Node]) = {
    <html>
      <head>
        <link href={ url("/stylesheets/site.css") } rel="stylesheet" type="text/css"/>
        <script src={ url("/javascripts/jquery-1.6.1.min.js") } type="text/javascript"></script>
        <script src={ url("/javascripts/socket.io.js") } type="text/javascript"></script>
        <script src={ url("/javascripts/app.js") } type="text/javascript"></script>
        <title>Rainy cloud</title>
        <style>{ style }</style>
      </head>
      <body>
        <div class="page">
          <div id="header">
            <div id="title"><h1>VENUS-C Management Application</h1></div>
            <div id="logindisplay">[<a href={ url("/login") }>Log On</a>]</div>
            <div id="menucontainer">
              <ul id="menu">
                <li>
                  <a href={ url("/") }>Home</a>
                </li>
                <li>
                  <a href={ url("/submit-test") }>Submit test</a>
                </li>
              </ul>
            </div>
          </div>
          <div id="main">
            { content }
          </div>
        </div>
      </body>
    </html>
  }

  def renderFile(name: String, ct: String = "text/css") = {
    contentType = ct
    io.Source.fromInputStream(getClass().getResourceAsStream(name)).mkString
  }

  Submitter.init

  get("/stylesheets/site.css") { renderFile("/stylesheets/site.css") }
  get("/javascripts/:name") { renderFile("/javascripts/"+params("name"), "text/javascript") }

  get("/") {
    render(<div>
             <h2>Spool</h2>
             <table>
               <tr>
                 <th>Queue length</th>
               </tr>
               <tr>
                 <td>
                   { Submitter.queueLength }
                 </td>
               </tr>
             </table>
             <h2>Workers</h2>
             <table>
               <tr>
                 <th>Id</th>
                 <th>Completed tasks</th>
                 <th>Last heartbeat</th>
                 <th>Uptime</th>
               </tr>
               {
                 for ((id, worker) <- Submitter.workers) yield <tr>
                                                                 <td>{ id }</td>
                                                                 <td>{ worker.completed }</td>
                                                                 <td>{ worker.heartbeatAgo / 1000 } s ago</td>
                                                                 <td>{ Utils.approximateTime(worker.uptime) }</td>
                                                               </tr>
               }
             </table>
             <h2>Jobs</h2>
             <table>
               <tr>
                 <th>Id</th>
                 <th>Submitted</th>
                 <th>Tasks</th>
                 <th>Status</th>
                 <th>Actions</th>
               </tr>
               {
                 for ((id, job) <- Submitter.jobs()) yield <tr>
                                                             <td>{ id }</td>
                                                             <td>Unknown</td>
                                                             <td>{ job.completedTasks }/{ job.totalTasks }</td>
                                                             <td>{ if (job.completed) "Completed" else "Running" }</td>
                                                             <td>
                                                               {
                                                                 if (job.completed)
                                                                   <a href={ url("/job/%s/delete".format(id)) }>Delete</a>
                                                                 else
                                                                   <a href={ url("/job/%s/kill".format(id)) }>Kill</a>
                                                               }
                                                             </td>
                                                           </tr>
               }
             </table>
           </div>)
  }

  get("/submit-test") {
    log.info("submitting test jobs")
    SubmitterTester.spawnTest()
    redirect(url("/"))
  }

  get("/job/:job/delete") {
    Submitter.deleteJob(params("job"))
    redirect(url("/"))
  }

  get("/job/:job/kill") {
    Submitter.killJob(params("job"))
    redirect(url("/"))
  }

}

class ZeromqMonitoringSocket extends ScalatraServlet  with SocketIOSupport {
  // socket io

 socketio { socket =>
    socket.onConnect { connection =>
      println("Connecting chat client [%s]" format connection.clientId)
      try {
        connection.send(SocketIOFrame.JSON_MESSAGE_TYPE, """{ "welcome": "Welcome to Socket IO chat" }""")
      } catch {
        case _ => connection.disconnect
      }
      connection.broadcast(SocketIOFrame.JSON_MESSAGE_TYPE,
        """{ "announcement": "New participant [%s]" }""".format(connection.clientId))
    }

    socket.onDisconnect { (connection, reason, _) =>
      println("Disconnecting chat client [%s] (%s)".format(connection.clientId, reason))
      connection.broadcast(SocketIOFrame.JSON_MESSAGE_TYPE,
        """{ "announcement": "Participant [%s] left" }""".format(connection.clientId))
    }

    socket.onMessage { (connection, _, message) =>
      println("RECV: [%s]" format message)
      message match {
        case "/rclose" => {
          connection.close
        }
        case "/rdisconnect" => {
          connection.disconnect
        }
        case _ => {
          connection.broadcast(SocketIOFrame.JSON_MESSAGE_TYPE,
            """{ "message": ["%s", "%s"] }""".format(connection.clientId, message))
        }
      }
    }
  }
}
