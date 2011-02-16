package it.cnr.aquamaps

import io.Source.fromFile

case class Partition(val start: String, val size: Long)

/** Creates a partition of the key space.
 * */
trait Partitioner {
  def partitions: Iterator[Partition]
}

class StaticPartitioner extends Partitioner {
  def toPartition(line: String) = {
    val Array(s, k) = line.trim split " "
    Partition(k, s.toInt)
  }

  val rangesFile = "octo/client/ranges"

  def partitions = fromFile(rangesFile).getLines map toPartition
}
