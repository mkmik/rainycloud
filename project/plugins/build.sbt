resolvers += "RI Snapshots" at "http://maven.research-infrastructures.eu/nexus/content/repositories/snapshots"

resolvers += "RI Releases" at "http://maven.research-infrastructures.eu/nexus/content/repositories/releases"

resolvers += "Tools" at "http://nexus.scala-tools.org/content/repositories/releases/"

resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

//libraryDependencies <+= sbtVersion(v => "com.eed3si9n" %% "sbt-assembly" % (v+"/0.7.1"))
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.7.3")

//resolvers += "Proguard plugin repo" at "http://siasia.github.com/maven2"

//libraryDependencies <+= sbtVersion("com.github.siasia" %% "xsbt-proguard-plugin" % _)


resolvers += "Web plugin repo" at "http://siasia.github.com/maven2"

//Following means libraryDependencies += "com.github.siasia" %% "xsbt-web-plugin" % "0.1.0-<sbt version>""
libraryDependencies <+= sbtVersion(v => "com.github.siasia" %% "xsbt-web-plugin" % (v+"-0.2.11"))

//libraryDependencies += "de.hars" %% "ensime-plugin" % "0.1"

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.5.2")
