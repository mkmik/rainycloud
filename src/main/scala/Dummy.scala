package it.cnr.aquamaps

import com.google.inject._
import com.google.inject.name._

class DummyHSPENLoader extends Loader[HSPEN] {
  def load = List()
}

class DummyGenerator @Inject() (val hspenLoader: Loader[HSPEN]) extends Generator {
  def computeInPartition(p: Partition) = println("dummy " + p)
}
