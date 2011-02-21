package it.cnr.aquamaps

/*!
 This is the Guice component wiring for a particular implementation scenario
 */

import com.google.inject._
import com.google.inject.name._
import uk.me.lings.scalaguice.ScalaModule
import net.lag.configgy.Config

case class AquamapsModule() extends AbstractModule with ScalaModule with RainyCloudModule {
  def configure() {
    bind[Config].toInstance(conf)
    /*!## Basic components

    We select partitioner which loads the pre-made partitions from a plain text file, an implementation of the `Generator` and
    `HspecAlgorithm` components.
    */
    bind[Partitioner].to[StaticPartitioner]
    bind[Generator].to[HSPECGenerator]
    bind[HspecAlgorithm].to[RandomHSpecAlgorithm]

    /*!## HSPEN database

    HSPEN data is loaded from a gzipped csv file located in the filesystem. Each worker should load this file once.
     */
    bind[Loader[HSPEN]].to[TableHSPENLoader]
    bind[TableReader[HSPEN]].toInstance(new FileSystemTableReader(conf.getString("hspenFile").getOrElse("data/hspen.csv.gz")))
    bind[PositionalSource[HSPEN]].to[CSVPositionalSource[HSPEN]]

    /*!## HCAF database
     
     In this wiring the `HCAF` database is loaded in memory from a gzipped csv file too, and the local worker
     will fetch a given partition as a slice of the whole `HCAF` db held in RAM. This is inefficient but works well
     for our first prototype.
     */
    bind[Loader[HCAF]].to[TableHCAFLoader]
    bind[TableReader[HCAF]].toInstance(new FileSystemTableReader(conf.getString("hcafFile").getOrElse("data/hcaf.csv.gz")))
    bind[PositionalSource[HCAF]].to[CSVPositionalSource[HCAF]]
    bind[Fetcher[HCAF]].to[MemoryFetcher[HCAF]].in[Singleton]

    /*!## Emitter
     
     The purpose of the emitter is to collect generated `HSPEC` records and write them somewhere. This emitter will write a CSV file.
     The file can be compressed (performance penalilty).
     */
    bind[TableWriter[HSPEC]].toInstance(new FileSystemTableWriter(conf.getString("hspecFile").getOrElse("/tmp/hspec.csv.gz")))
    bind[PositionalSink[HSPEC]].to[CSVPositionalSink[HSPEC]].in[Singleton]
    bind[Emitter[HSPEC]].to[CSVEmitter[HSPEC]].in[Singleton]

    /*!## Octobot

     (ignore this)
     This is the octobot entry point, which is loaded from a different entry point (in fact we could move this to separate guice module)
     */
    bind[Bot].to[HSPECGeneratorOctobot]
  }
}
