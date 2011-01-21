package it.cnr.aquamaps

import com.google.inject._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule

case class AquamapsModule() extends AbstractModule with ScalaModule {
  def configure() {
    bind[Partitioner].to[StaticPartitioner]
    bind[Generator].to[DummyGenerator]

    bind[HSPENLoader].to[TableHSPENLoader]

    bind[String].annotatedWith[Hspen].toInstance("data/hspen.csv.gz")
    bind[String].annotatedWith[Hcaf].toInstance("data/hcaf.csv.gz")

    bind[TableReader].to[FileSystemTableReader]
    bind[TableLoader].to[CSVTableLoader]

    // For octobot worker (different entry point)
    bind[Bot].to[HSPECGeneratorOctobot]
  }
}
