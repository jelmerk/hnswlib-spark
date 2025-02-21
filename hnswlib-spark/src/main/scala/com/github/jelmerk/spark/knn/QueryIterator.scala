package com.github.jelmerk.spark.knn

import java.net.InetSocketAddress

import scala.util.Try
import scala.util.control.NonFatal

import com.github.jelmerk.registration.server.PartitionAndReplica
import com.github.jelmerk.serving.client.IndexClientFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

private[knn] class QueryIterator[TId, TVector, TDistance](
    indices: Map[PartitionAndReplica, InetSocketAddress],
    indexClientFactory: IndexClientFactory[TId, TVector, TDistance],
    records: Iterator[Row],
    outputSchema: StructType,
    batchSize: Int,
    k: Int,
    vectorCol: String,
    partitionsCol: Option[String]
) extends Iterator[Row] {

  private var failed = false
  private val client = indexClientFactory.create(indices)

  private val delegate =
    if (records.isEmpty) Iterator[Row]()
    else
      records
        .grouped(batchSize)
        .map(batch => client.search(vectorCol, partitionsCol, batch, outputSchema, k))
        .reduce((a, b) => a ++ b)

  override def hasNext: Boolean = delegate.hasNext

  override def next(): Row = {
    if (failed) {
      throw new IllegalStateException("Client shutdown.")
    }
    try {
      val result = delegate.next()

      if (!hasNext) {
        client.shutdown()
      }
      result
    } catch {
      case NonFatal(t) =>
        Try(client.shutdown())
        failed = true
        throw t
    }
  }
}
