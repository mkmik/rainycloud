package it.cnr.aquamaps

import com.google.inject.Module
import net.lag.configgy.Config
import net.lag.configgy.Configgy

trait RainyCloudModule {
  val conf = if (Configgy.config != null) Configgy.config else Config.fromString("")
}

object Modules {
  val modules = Map("BabuDB" -> BabuDBModule(),
    "COMPSs" -> COMPSsModule(),
    "HDFS" -> HDFSModule())

  def enabledModules(conf: Config): Seq[Module] = for {
    name <- conf.getList("modules")
    module <- modules.get(name)
  } yield module

}
