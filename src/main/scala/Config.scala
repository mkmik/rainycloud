package it.cnr.aquamaps

import com.google.inject._
import uk.me.lings.scalaguice.ScalaModule

case class AquamapsModule() extends AbstractModule with ScalaModule {
  def configure() {
    bind[Partitioner].to[StaticPartitioner]
    bind[Generator].to[DummyGenerator]
    bind[Bot].to[HSPECGeneratorOctobot]
  }
}
