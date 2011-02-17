package it.cnr.aquamaps

import com.google.inject._
import com.google.inject.name._

class DummyHSPENLoader extends HSPENLoader {
  def load = List()
}

class DummyGenerator @Inject() (val hspenLoader: HSPENLoader) extends Generator {
  def computeInPartition(p: Partition) = println("dummy " + p)
}
