package it.cnr.aquamaps

import com.google.inject._
import uk.me.lings.scalaguice.InjectorExtensions._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import net.lag.configgy.Config

import java.io.File

/*!## Wiring

  If we want to use COMPs we should use this Guice wiring configuration.
  This configuration is meant to override the AquamapModule in [Config.scala](Config.scala.html).*/

case class COMPSsModule() extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {

    /*! This overrides the default `Generator` to use a specific wrapper for COMPSs. The `COMPsGenerator` converts the parameters into file and then delegates
     the rest of the work toa FileParamsGenerator. */
    bind[Generator].to[COMPSsGenerator]

    /*! The `StaticFileParamsGenerator` invokes a static method with a filename parameters, we can configure COMPSs to use that place as pointcut. */
    bind[FileParamsGenerator].to[StaticFileParamsGenerator]

    bind[Emitter[HSPEC]].to[COMPSsCollectorEmitter[HSPEC]]

  }

  @Provides @Singleton
  def emitter(tableWriter: TableWriter[HSPEC]): COMPSsCollectorEmitter[HSPEC] = new COMPSsCollectorEmitter(tableWriter)

}

/*!## Wiring

 This case is even easier, we can handle the Object-passing convention by just overriding the Generator
 */
case class COMPSsObjectModule() extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {

    /*! This overrides the default `Generator` to use a specific wrapper for COMPSs. The `COMPsGenerator` converts the parameters into file and then delegates
     the rest of the work toa FileParamsGenerator. */
    bind[Generator].to[COMPSsObjectGenerator]

    bind[ObjectParamsGenerator[HSPEC]].to[StaticObjectParamsGenerator]
  }

}
