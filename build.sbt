import AssemblyKeys._ // put this at the top of the file

seq(assemblySettings: _*)

seq(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

name := "rainycloud"

version := "1.2.3"

organization := "it.cnr"

scalaVersion := "2.9.0"

publishTo := Some("RI Releases" at "http://maven.research-infrastructures.eu/nexus/content/repositories/snapshots/")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishMavenStyle := true

seq(webSettings :_*)

//seq(ProguardPlugin.proguardSettings :_*)

//proguardOptions ++= Seq(
//  keepAllScala, keepMain("it.cnr.aquamaps.Main")
//)

scalacOptions += "-unchecked"

scalacOptions += "-deprecation"

// disable updating dynamic revisions (including -SNAPSHOT versions)
offline := true

// disable using the Scala version in output paths and artifacts
crossPaths := false

// fork a new JVM for 'run' and 'test:run'
fork := true

javaOptions in run += "-javaagent:/home/marko/bin/ZeroTurnaround/JRebel/jrebel.jar"

resolvers += ScalaToolsSnapshots

resolvers += JavaNet1Repository

resolvers += "RI Releases" at "http://maven.research-infrastructures.eu/nexus/content/repositories/releases"

resolvers += "RI Snapshots" at "http://maven.research-infrastructures.eu/nexus/content/repositories/snapshots"

resolvers += "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "OSGeo" at "http://download.osgeo.org/webdav/geotools/"

resolvers += "OpenGeo" at "http://repo.opengeo.org/"

resolvers += "Twitter" at "http://maven.twttr.com/"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

//resolvers += "Akka" at "http://akka.io/repository/"

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % "3.0",
  "com.google.inject.extensions" % "guice-servlet" % "3.0",
  "uk.me.lings" % "scala-guice_2.8.0" % "0.1",
  "net.lag" % "configgy" % "2.0.2-nologgy" % "compile", //ApacheV2
  "com.weiglewilczek.slf4s" %% "slf4s" % "1.0.7-SNAPSHOT",
  "com.github.scopt" %% "scopt" % "1.0.0-SNAPSHOT",
  "com.github.jsuereth.scala-arm" %% "scala-arm" % "0.2",
  "net.sf.opencsv" % "opencsv" % "2.1",
  "org.supercsv" % "supercsv" % "1.20",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2",
  "org.geoscript" % "library_2.8.0" % "0.6.1",
  "com.typesafe.akka" % "akka-actor" % "2.0-RC2",
  "com.typesafe.akka" % "akka-agent" % "2.0-RC2"
)


{
  val scalatraVersion = "2.0.0-SNAPSHOT"
  libraryDependencies ++= Seq(
    "org.scalatra" %% "scalatra" % scalatraVersion,
    "org.scalatra" %% "scalatra-scalate" % scalatraVersion,
    "org.mortbay.jetty" % "servlet-api" % "2.5-20081211" % "provided"
  )
}

libraryDependencies += "com.google.code.gson" % "gson" % "1.7.1"

libraryDependencies += "net.sf.json-lib" % "json-lib" % "2.3" classifier "jdk15"

libraryDependencies += "org.scala-tools" % "vscaladoc" % "1.1"



libraryDependencies ++= Seq(
  "org.mockito" % "mockito-all" % "1.8.5"
)


mainClass in (Compile, packageBin) := Some("it.cnr.aquamaps.Main")

// we can customize the run
mainClass in (Compile, run) := Some("it.cnr.aquamaps.Main")

// we can customize the run
//mainClass in (Compile, assembly) := Some("it.cnr.aquamaps.Main")

test in assembly := {}


libraryDependencies ++= Seq(
//  "net.liftweb" %% "lift-webkit" % "2.3" % "compile",
  "org.mortbay.jetty" % "jetty" % "6.1.22" % "container",
  "ch.qos.logback" % "logback-classic" % "0.9.26",
  "joda-time" % "joda-time" % "1.6.2",
  "postgresql" % "postgresql" % "8.4-701.jdbc4",
  "com.traveas" %% "querulous-light" % "0.0.6"
)
