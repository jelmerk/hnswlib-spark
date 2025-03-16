package com.github.jelmerk.index

import java.net.InetSocketAddress
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.{mutable, Seq}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Random

import com.github.jelmerk.registration.PartitionAndReplica
import com.github.jelmerk.spark.knn.Codec
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver
import org.apache.spark.sql.Row

final case class PartitionSummary(partition: Int, size: Int, hosts: Seq[String])

class IndexClient[TId, TVector, TDistance](
    indexAddresses: Map[PartitionAndReplica, InetSocketAddress],
    vectorConverter: (String, Row) => Vector,
    idCodec: Codec[TId, Id],
    distanceCodec: Codec[TDistance, Distance]
)(implicit distanceOrdering: Ordering[TDistance]) {

  private val random = new Random()

  private val (
    channels: Seq[ManagedChannel],
    grpcClients: Seq[(PartitionAndReplica, IndexServiceGrpc.IndexServiceStub)]
  ) = indexAddresses.toSeq.map { case (key, address) =>
    val channel = NettyChannelBuilder
      .forAddress(address)
      .usePlaintext
      .build()

    (channel, (key, IndexServiceGrpc.stub(channel)))
  }.unzip

  private val partitionClients: IndexedSeq[IndexedSeq[IndexServiceGrpc.IndexServiceStub]] = grpcClients
    .groupBy(_._1.partitionNum)
    .map { case (partition, it) => partition -> it.map(_._2).toIndexedSeq }
    .toIndexedSeq
    .sortBy(_._1)
    .map(_._2)

  private val threadPool = Executors.newFixedThreadPool(1)

  def search(
      vectorColumn: String,
      queryPartitionsColumn: Option[String],
      batch: Seq[Row],
      k: Int
  ): Iterator[Row] = {

    val queries = batch.map { row =>
      val partitions = queryPartitionsColumn.map(row.getAs[Seq[Int]])
      val vector     = vectorConverter(vectorColumn, row)
      (partitions, vector)
    }

    // pick a random replica for every partition
    val randomClients = partitionClients.map { clients => clients(random.nextInt(clients.size)) }

    val (requestObservers, responseIterators) = randomClients.map { client =>
      val responseStreamObserver = new StreamObserverAdapter[SearchResponse]()
      val requestStreamObserver  = client.search(responseStreamObserver)

      (requestStreamObserver, responseStreamObserver: Iterator[SearchResponse])
    }.unzip

    threadPool.submit(new Runnable {
      override def run(): Unit = {
        val queriesIterator = queries.iterator

        for {
          (queryPartitions, vector)     <- queriesIterator
          last                           = !queriesIterator.hasNext
          (observer, observerPartition) <- requestObservers.zipWithIndex
        } {
          if (queryPartitions.fold(true)(_.contains(observerPartition))) {
            val request = SearchRequest(Some(vector), k)
            observer.onNext(request)
          }
          if (last) {
            observer.onCompleted()
          }
        }
      }
    })

    val expectations = batch.map { row =>
      val partitions = queryPartitionsColumn.map(row.getAs[Seq[Int]])
      partitions -> row
    }.iterator

    new ResultsIterator(expectations, responseIterators, k)
  }

  def saveIndex(path: String): Unit = {
    val futures = partitionClients.zipWithIndex.flatMap { case (clients, partition) =>
      // only the primary replica saves the index
      clients.headOption.map { client =>
        val request = SaveIndexRequest(s"$path/$partition")
        client.saveIndex(request)
      }
    }

    Await.result(Future.sequence(futures), Duration.Inf)
  }

  def partitionSummaries(): Seq[PartitionSummary] = {
    val futures = partitionClients.zipWithIndex.flatMap { case (clients, partition) =>
      clients.headOption.map { client =>
        val request = SummaryRequest()
        client.summary(request).map { resp =>
          val hosts = indexAddresses.toList.collect {
            case (PartitionAndReplica(id, _), addr) if partition == id => s"${addr.getHostName}:${addr.getPort}"
          }
          PartitionSummary(partition, resp.size, hosts)
        }
      }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  def shutdown(): Unit = {
    channels.foreach(_.shutdown())
    threadPool.shutdown()
  }

  private class StreamObserverAdapter[T] extends StreamObserver[T] with Iterator[T] {

    private val queue   = new LinkedBlockingQueue[Either[Throwable, T]]
    private val counter = new AtomicInteger()
    private val done    = new AtomicBoolean(false)

    // ======================================== StreamObserver ========================================

    override def onNext(value: T): Unit = {
      queue.add(Right(value))
      counter.incrementAndGet()
    }

    override def onError(t: Throwable): Unit = {
      queue.add(Left(t))
      done.set(true)
    }

    override def onCompleted(): Unit = {
      done.set(true)
    }

    // ========================================== Iterator ==========================================

    // Technically this is incorrect because it never completes but because ResultsIterator orchestrates everything
    // this should not be an issue
    override def hasNext: Boolean = !done.get()

    override def next(): T = queue.take() match {
      case Right(value) => value
      case Left(t)      => throw t
    }
  }

  private class ResultsIterator(
      iterator: Iterator[(Option[Seq[Int]], Row)],
      partitionIterators: IndexedSeq[Iterator[SearchResponse]],
      k: Int
  ) extends Iterator[Row] {

    override def hasNext: Boolean = iterator.hasNext

    override def next(): Row = {
      val (partitions, row) = iterator.next()

      val responses = partitions match {
        case Some(parts) => parts.map(partitionIterators.apply).map(_.next())
        case _           => partitionIterators.map(_.next())
      }

      val allResults = for {
        response <- responses
        result   <- response.results
      } yield idCodec.decode(result.getId) -> distanceCodec.decode(result.getDistance)

      val results = topK(allResults, k).map { case (id, distance) => Row(id, distance) }

      val n      = row.length
      val values = Array.ofDim[Any](n + 1)
      var i      = 0
      while (i < n) {
        values.update(i, row.get(i))
        i += 1
      }
      values.update(i, results)

      // ArraySeq.unsafeWrapArray is only available in 2.13 and toIndexedSeq will be too slow
      @annotation.nowarn("cat=deprecation")
      val result = Row(values: _*)
      result
    }
  }

  private def topK(allResults: Seq[(TId, TDistance)], k: Int): Seq[(TId, TDistance)] = {
    val pq = mutable.PriorityQueue[(TId, TDistance)]()(distanceOrdering.on[(TId, TDistance)](_._2).reverse)
    allResults.foreach { case (id, distance) =>
      if (pq.size < k) pq.enqueue((id, distance))
      else
        pq.headOption.foreach { case (_, biggestDistance) =>
          if (distanceOrdering.lt(distance, biggestDistance)) {
            pq.dequeue()
            pq.enqueue((id, distance))
          }
        }
    }
    pq.dequeueAll
  }
}

class IndexClientFactory[TId, TVector, TDistance](
    vectorConverter: (String, Row) => Vector,
    idCodec: Codec[TId, Id],
    distanceCodec: Codec[TDistance, Distance]
)(implicit distanceOrdering: Ordering[TDistance])
    extends Serializable {

  def create(servers: Map[PartitionAndReplica, InetSocketAddress]): IndexClient[TId, TVector, TDistance] =
    new IndexClient(servers, vectorConverter, idCodec, distanceCodec)

}
