package it.cnr.aquamaps

import io.Source.fromFile

/*!
 
 A partitioner creates a partition of the key space. It returns partitions which are describes by ranges over keys.
 */
trait Partitioner {
  def partitions: Iterator[Partition]
}

case class Partition(val start: String, val size: Long)


/*!

 The simples partitioner is the StaticPartitioner, which simply reads key ranges from a precomputed file
 */
class StaticPartitioner extends Partitioner {
  def toPartition(line: String) = {
    val Array(s, k) = line.trim split " "
    Partition(k, s.toInt)
  }

  val rangesFile = "octo/client/rangesSmall"

  def partitions = fromFile(rangesFile).getLines map toPartition
}
