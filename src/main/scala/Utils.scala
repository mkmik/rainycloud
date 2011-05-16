package it.cnr.aquamaps

import net.lag.configgy.Configgy


object Utils {
  def confGetString(key: String, default: String) = Configgy.config.getString(key).getOrElse(default)
}
