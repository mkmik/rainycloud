package it.cnr.aquamaps

import com.google.inject._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

case class AquamapsModule() extends AbstractModule with ScalaModule {
  def configure() {
    bind[Partitioner].to[StaticPartitioner]
    bind[Generator].to[DummyGenerator]

    bind[HSPENLoader].to[TableHSPENLoader]

    bind[TableReader[HSPEN]].toInstance(new FileSystemTableReader("data/hspen.csv.gz"))
    bind[TableLoader[HSPEN]].to[CSVTableLoader[HSPEN]]

    // For octobot worker (different entry point)
    bind[Bot].to[HSPECGeneratorOctobot]
  }
}
