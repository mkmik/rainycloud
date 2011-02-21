package it.cnr.aquamaps

import com.google.inject.Module
import net.lag.configgy.Config


object Modules {
  val modules = Map("BabuDB" -> BabuDBModule(),
                    "COMPSs" -> COMPSsModule())


  def enabledModules(conf: Config): Seq[Module] = for (name <- conf.getList("modules");
                                                         module <- modules.get(name))
                                                    yield module

}
