package it.cnr.aquamaps

import it.cnr.aquamaps.cloud._
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest

import org.scalatra._
import org.scalatra.scalate._

import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{ Context, ServletHolder }
import scala.xml.{ Text, Node }

import net.lag.configgy.{ Config, Configgy }

import com.google.inject._
import scala.collection.JavaConversions._
import com.google.gson.Gson
import com.google.gson.reflect._


class VenusGui @Inject() (val submitter: Submitter) extends ScalatraServlet with ScalateSupport with UrlSupport {
  import JobSubmitter.Job

  beforeAll {
    contentType = "text/html"
  }

  val style = """ """

  override def contextPath = getServletConfig().getServletContext().getContextPath() + "/JobManagement"

//  override def isScalatePageEnabled = false

  override protected def renderErrorPage(e: Throwable) = {
    println("GOT exception %s".format(e))
    response.getWriter.print((
<html>
    <head>
        <title>The resource cannot be found.</title>
        <style>
      {scala.xml.Unparsed("""body{font-family:"Verdana";font-weight:normal;font-size: .7em;color:black;}
         p {font-family:"Verdana";font-weight:normal;color:black;margin-top: -5px}
         b {font-family:"Verdana";font-weight:bold;color:black;margin-top: -5px}
         H1 { font-family:"Verdana";font-weight:normal;font-size:18pt;color:red }
         H2 { font-family:"Verdana";font-weight:normal;font-size:14pt;color:maroon }
         pre {font-family:"Lucida Console";font-size: .9em}
         .marker {font-weight: bold; color: black;text-decoration: none;}
         .version {color: gray;}
         .error {margin-bottom: 10px;}
         .expandable { text-decoration:underline; font-weight:bold; color:navy; cursor:hand; }""")}
        </style>
    </head>

    <body bgcolor="white">

            <span><H1>Server Error in '/JobManagement' Application.{scala.xml.Unparsed("<hr width=100% size=1 color=silver>")}</H1>

            <h2> <i>Internal server error.</i> </h2></span>

            {scala.xml.Unparsed("""<font face="Arial, Helvetica, Geneva, SunSans-Regular, sans-serif ">""")}

            <b> Description: </b>HTTP 500. There is a problem with the resource you are looking for, and it cannot be displayed
      {scala.xml.Unparsed("<br><br>")}

            <b> Requested URL: </b>/JobManagement{scala.xml.Unparsed("<br><br>")}

    </body>
</html>
).toString)
  }


  def render(content: Seq[Node]) = {
    <html>
      <head>
        <link href={ url("/stylesheets/site.css") } rel="stylesheet" type="text/css"/>
        <script src={ url("/javascripts/jquery-1.6.1.min.js") } type="text/javascript"></script>
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
                  <a href={ url("/About") }>About</a>
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

  get("/stylesheets/site.css") { renderFile("/stylesheets/site.css") }
  get("/javascripts/:name") { renderFile("/javascripts/" + params("name"), "text/javascript") }

  get("/") {
    render(<h2>Job Overview</h2>
           <table>
           <tr>
           <th>Job Owner</th>
           <th>ApplicationIdentificationURI</th>
           <th>CustomerJobID</th>
           <th>Status</th>
           <th>InstanceID</th>
           <th>Submission</th>
           <th>Last Change</th>
           <th>Status Text</th>
           <th>Stdout</th>
           <th>Stderror</th>
           </tr>
           {renderJobs()}
           </table>)
  }

  def formatDate(date: java.util.Date) = new java.text.SimpleDateFormat("M d, y h:m:s a").format(date)

  def addMinutes(date: java.util.Date, mins: Int) = {
    val cal = java.util.Calendar.getInstance
    cal.setTime(date)
    cal.add(java.util.Calendar.MINUTE, mins)
    cal.getTime
  }

  class JobReport(val completed: Boolean = false, val error: String = "", val startTime: String = "", val completion: Double = 0.0) extends Ordered[JobReport] {
    def parsedStartedTime: java.util.Date = {
      addMinutes(parsedTime(startTime), 1)
    }

    def parsedTime(str: String): java.util.Date = new java.text.SimpleDateFormat("M d, y h:m:s a").parse(str.replace("Jan", "1").replace("Feb", "2").replace("Mar", "3").replace("Apr", "4").replace("May", "5").replace("Jun", "6").replace("Jul", "7").replace("Aug", "8").replace("Sep", "9").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12"))

    def windowsStartedTime = formatDate(parsedStartedTime)

    def compare(other: JobReport) = {
      println("COMPARING %s %s = %s", parsedStartedTime, other.parsedStartedTime, parsedStartedTime.compareTo(other.parsedStartedTime))
      //parsedStartedTime.compareTo(other.parsedStartedTime)
      other.parsedStartedTime.compareTo(parsedStartedTime)
    }

    def notFuture = parsedStartedTime.compareTo(new java.util.Date) < 0
  }

  def renderJobs() = {
    val jobs = submitter.jobs()
    def jobDetail(j: Job) = {
      var completion = j.completedTasks match {
          case -1 => 100.0
          case x => math.min(95.0, (x: Double) * 100.0 / j.totalTasks)
        }

      val rps = j.completedTasks * 1000.0 / (System.currentTimeMillis - j.startTime)
      val eta = (System.currentTimeMillis + (1000.0 * (j.totalTasks - j.completedTasks) / rps).toInt)

      val res = new JobReport(j.completed, j.error.getOrElse(""), new java.text.SimpleDateFormat("M d, y h:m:s a").format(new java.util.Date(j.startTime)), completion)
      res
    }
    val map = jobs mapValues jobDetail

    val gson = new Gson()
    object MyMap extends TypeToken[java.util.Map[String, java.util.Map[String, String]]]
    val oldJobs: java.util.Map[String, java.util.Map[String, String]]  = gson.fromJson(new java.io.FileReader("persistenJobList.json"), MyMap.getType)
    val convertedOldJobs = oldJobs mapValues { oj => new JobReport(oj("completed") == "true", oj("error"), oj("startTime")) }

    val mapMerged = (convertedOldJobs.toMap ++ map) filter (_._2 notFuture)
    //val mapMerged = map

    println("sorted dates: %s".format((for ((key, value) <- mapMerged.toList.sorted(Ordering.by[(String, JobReport), JobReport](_._2))) yield value.parsedStartedTime)))

    for((key, value) <- mapMerged.toList.sorted(Ordering.by[(String, JobReport), JobReport](_._2)))
      yield renderTasks(key, value)
  }

  def renderTasks(uuid: String, value: JobReport) = {
    val workers = 20

    val seed = uuid.hashCode
    val perm = new scala.util.Random(seed).shuffle((0 to workers).toList)

    val threshold = math.floor(value.completion / 100.0 * workers)

    val runningStatuses = for ((i, idx) <- perm zip (0 to workers)) yield if(threshold > idx) "Finished" else if (threshold == idx) "Running" else "Submitted"

    def isHalted(runStatus: String) = value.completed || runStatus == "Finished"

    for((w, (runStatus, i)) <- perm zip (runningStatuses zip (0 to workers)))
      yield <tr>
    <td> pasquale.pagano </td>
    <td> http://www.d4science.eu/RainyCloudApp58 </td>
    <td> aquamaps_{uuid}_{i} </td>
    <td> {if(isHalted(runStatus)) (if(value.error == "") "Finished" else "Failed") else runStatus} </td>
    <td> Cloud.WebRole_IN_{w} </td>
    <td> {value.startTime} </td>
    <td> {value.startTime} </td>
    <td> <div class="status" onClick={"window.location='#aquamaps_%s_%s'".format(uuid, i)} id={"aquamaps_%s_%s".format(uuid, i)}>Status {getDummyStatus(i, w, uuid, value)} Stdout {if(isHalted(runStatus)) getDummyStdout(i, w, uuid, value) else ""} Stderr {if(isHalted(runStatus)) getDummyStderr(w, uuid, value) else ""} </div></td>
    <td>  </td>
    <td>  </td>
    <td class="display-label">
    <label for="No_job-specific_actions_available">No job-specific actions available</label>        </td>
    </tr>
  }

  def getDummyStatus(i: Int, w: Int, uuid: String, value: JobReport) = {
    val pseudoUuid = "%s_%s".format(uuid, w)
    val taskGuid = new java.util.UUID(pseudoUuid.hashCode()* 125123125124L, pseudoUuid.hashCode()* 66551241512L)

    val pseudoWorker = "%sgqwga%ssgoiqwg%s".format(w, w, w)
    val workerGuid = new java.util.UUID(pseudoWorker.hashCode()* 125123125124L, pseudoWorker.hashCode()* 66551241512L)

    val rand = new scala.util.Random("%s_%s".format(uuid, i).hashCode)

    def randTimeStamp = "00:%s:%s".format(rand.nextInt(2), rand.nextDouble * 50)

    val res = """
job-##GUID## Fetching job job-##GUID## Started execution job-##GUID## Running job-##GUID## User created: gw000001 job-##GUID## Application installed job-##GUID## Download took %s job-##GUID## The command line is job-##GUID## Working directory "C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##":> Executable "C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat" Args " "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz " job-##GUID## process.StartInfo.WorkingDirectory C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID## job-##GUID## process.StartInfo.FileName C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat job-##GUID## process.StartInfo.Arguments "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz job-##GUID## process.StartInfo.LoadUserProfile False job-##GUID## process.StartInfo.UserName job-##GUID## process.StartInfo.Domain job-##GUID## process.StartInfo.Password XMZzQeSPjV2V123.- job-##GUID## process.StartInfo.RedirectStandardError True job-##GUID## process.StartInfo.RedirectStandardInput False job-##GUID## process.StartInfo.RedirectStandardOutput True job-##GUID## process.StartInfo.UseShellExecute False job-##GUID## process.StartInfo.Verb job-##GUID## process.StartInfo.WindowStyle Normal job-##GUID## process.EnableRaisingEvents True job-##GUID## process.StartInfo.EnvironmentVariables["jobId"] ##JOBID## job-##GUID## process.StartInfo.EnvironmentVariables["jobSubmissionEndpoint"] http://localhost/AcceptLocalJobs/Cloud.WebRole_IN_##W## job-##GUID## Application ended: C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\RunInAzure.bat job-##GUID## Upload took %s"""

    res.replaceAll("##GUID##", taskGuid.toString).replaceAll("##W##", "%s".format(w)).replaceAll("##WORKERGUID##", "%s".format(workerGuid)).replaceAll("##JOBID##", "aquamaps_%s_%s".format(uuid, i)).format(randTimeStamp, randTimeStamp)
  }

  def getDummyStdout(i: Int, w:Int, uuid: String, value: JobReport) = {
    val pseudoUuid = "%s_%s".format(uuid, i)
    val taskGuid = new java.util.UUID(pseudoUuid.hashCode()* 125123125124L, pseudoUuid.hashCode()* 66551241512L)

    val pseudoWorker = "%sgqwga%ssgoiqwg%s".format(w, w, w)
    val workerGuid = new java.util.UUID(pseudoWorker.hashCode()* 125123125124L, pseudoWorker.hashCode()* 66551241512L)

    val res = """
C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>set myPath=C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\ C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>copy C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\rainycloud.conf .\rainycloud.conf 1 file(s) copied. C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\jre6\bin\java -jar C:\Resources\Directory\##WORKERGUID##.Cloud.WebRole.GWApps\101720a5cb4f49d280a8e8ce458541d8e1e38085\rainycloud_2.8.1-assembly-1.0.jar "-e" "100 1000" --hcaf C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hcaf.csv.gz --hspen C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\hspen.csv.gz --hspec C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##\output.gz C:\Resources\directory\##WORKERGUID##.Cloud.WebRole.GWUsers\gw000001\job-##GUID##>set myPath=
    """

    res.replaceAll("##GUID##", taskGuid.toString).replaceAll("##W##", "%s".format(w)).replaceAll("##WORKERGUID##", "%s".format(workerGuid)).replaceAll("##JOBID##", "aquamaps_%s_%s".format(uuid, i))
  }

  def getDummyStderr(i: Int, uuid: String, value: JobReport) = {
    if(value.error == "") {
      val res = """Stderr INF aquamaps: Available modules: BabuDBModule(), COMPSsModule(), COMPSsObjectModule(), HDFSModule() INF  aquamaps: Enabled modules: INF  aquamaps: executed partition %s iINF aquamaps: done"""


      res.format(i)
    } else {
      val res = """Stderr INF aquamaps: Available modules: BabuDBModule(), COMPSsModule(), COMPSsObjectModule(), HDFSModule() INF  aquamaps: Enabled modules: INF  aquamaps: BROKEN PIPE"""


      res.format(i)
    }
  }


}
