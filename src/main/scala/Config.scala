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
    bind[PositionalStore[HSPEN]].to[CSVPositionalStore[HSPEN]]

    bind[HCAFLoader].to[TableHCAFLoader]
    bind[TableReader[HCAF]].toInstance(new FileSystemTableReader("data/hcaf.csv.gz"))
    bind[PositionalStore[HCAF]].to[CSVPositionalStore[HCAF]]

    bind[Fetcher[HCAF]].to[MemoryFetcher[HCAF]]

    // For octobot worker (different entry point)
    bind[Bot].to[HSPECGeneratorOctobot]
  }
}
