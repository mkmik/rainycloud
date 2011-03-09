import sbt._
import reaktor.scct.ScctProject

//import eu.dnetlib.DoccoPlugin

import xsbt.ScalaInstance

import java.io.File

class RainyCloudProject(info: ProjectInfo) extends DefaultProject(info) with AssemblyProject with ScctProject with AutoCompilerPlugins {
  val log4j = "log4j" % "log4j" % "1.2.16"

  //val scromiumRepo = "Cliff's Scromium Repo" at "http://cliffmoon.github.com/scromium/repository/"

  //	val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  //val scromium = "scromium" % "scromium_2.8.0" % "0.6.4" // artifacts Artifact("scromium-all_2.8.0", "all", "jar")

  override def managedStyle = ManagedStyle.Maven

  lazy val publishTo = Resolver.url("RI Releases", new java.net.URL("http://maven.research-infrastructures.eu/nexus/content/repositories/snapshots/"))
  Credentials(Path.userHome / ".ivy2" / ".credentials", log)

  val riReleases = "RI Releases" at "http://maven.research-infrastructures.eu/nexus/content/repositories/releases"
  val scalaToolsSnapshots = "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"
  val scalaToolsReleases = "Scala-Tools Maven2 Release Repository" at "http://scala-tools.org/repo-releases"

  val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
//  val fuseRepo = "fuse repo" at "http://repo.fusesource.com/maven2-all/"
  val akkaRepo = "akka repo" at "http://akka.io/repository"
  //val fruit    = "guiceyfruit repo" at "http://guiceyfruit.googlecode.com/svn/repo/releases/"

  // avro
  val radlabRepo = "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"
  val avroScala = compilerPlugin("com.googlecode" % "avro-scala-compiler-plugin" % "1.1-SNAPSHOT")
  val pluginRuntime = "com.googlecode" % "avro-scala-compiler-plugin" % "1.1-SNAPSHOT"
  val avro = "org.apache.hadoop" % "avro" % "1.3.3"
  private val pluginDeps = Set("avro-1.3.3.jar", "jackson-core-asl-1.4.2.jar", "jackson-mapper-asl-1.4.2.jar")

  override def getScalaInstance(version: String) = {
    val pluginJars = compileClasspath.filter(path => pluginDeps.contains(path.name)).getFiles.toSeq
    withExtraJars(super.getScalaInstance(version), pluginJars)
  }

  def withExtraJars(si: ScalaInstance, extra: Seq[File]) =
    ScalaInstance(si.version, si.libraryJar, si.compilerJar, info.launcher, extra: _*)

  // testing 
  val specsdep = "org.scala-tools.testing" %% "specs" % "1.6.7.2" % "test->default"
  val mockito = "org.mockito" % "mockito-all" % "1.8.5"

  val metrics = "com.yammer" %% "metrics" % "1.0.7" withSources ()
  val guice = "com.google.inject" % "guice" % "3.0-rc2"
  val guiceScala = "uk.me.lings" % "scala-guice_2.8.0" % "0.1"

  val configgy = "net.lag" % "configgy" % "2.0.2-nologgy" % "compile" //ApacheV2
  val scopt = "eed3si9n" %% "scopt" % "1.0"
  val scalaArm = "com.github.jsuereth.scala-arm" %% "scala-arm" % "0.2"

  val opencsv = "net.sf.opencsv" % "opencsv" % "2.1"
  val supercsv = "org.supercsv" % "supercsv" % "1.20"

  val hadoop = "org.apache.hadoop" % "hadoop-core" % "0.20.2"

  override def mainClass = Some("it.cnr.aquamaps.Main")
}
