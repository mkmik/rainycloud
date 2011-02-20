package it.cnr.aquamaps

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import com.google.inject.util.Modules

import java.io.File

/*!## Wiring

  If we want to use COMPs we should use this Guice wiring configuration.
  This configuration is meant to override the AquamapModule in [Config.scala](Config.scala.html).*/

case class COMPSsModule() extends AbstractModule with ScalaModule {
  def configure() {

    /*! This overrides the default `Generator` to use a specific wrapper for COMPSs. The `COMPsGenerator` converts the parameters into file and then delegates
     the rest of the work toa FileParamsGenerator. */
    bind[Generator].to[COMPSsGenerator]

    /*! The `StaticFileParamsGenerator` invokes a static method with a filename parameters, we can configure COMPSs to use that place as pointcut. */
    bind[FileParamsGenerator].to[StaticFileParamsGenerator]

    /*! Unfortunately we need to obtain the filename from the writer. So we need to declare a single TableWriter instance as bound on two different types
     otherwise Guice will not resolve the injections */
    bind[TableWriter[HSPEC]].to[FileSystemTableWriter[HSPEC]]

  }

  @Provides
  def writer(): FileSystemTableWriter[HSPEC] = new FileSystemTableWriter("/tmp/hspec.csv.gz")

}

