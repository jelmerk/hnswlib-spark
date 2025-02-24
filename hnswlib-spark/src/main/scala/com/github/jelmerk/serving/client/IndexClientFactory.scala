package com.github.jelmerk.serving.client

import java.net.InetSocketAddress
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.{mutable, Seq}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Random

import com.github.jelmerk.registration.server.PartitionAndReplica
import com.github.jelmerk.server.index._
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver
import org.apache.spark.sql.Row

class IndexClient[TId, TVector, TDistance](
    indexAddresses: Map[PartitionAndReplica, InetSocketAddress],
    vectorConverter: (String, Row) => SearchRequest.Vector,
    idExtractor: Result => TId,
    distanceExtractor: Result => TDistance
)(implicit distanceOrdering: Ordering[TDistance]) {

  private val random = new Random()

  private val (channels, grpcClients) = indexAddresses.map { case (key, address) =>
    val channel = NettyChannelBuilder
      .forAddress(address)
      .usePlaintext
      .build()

    (channel, (key, IndexServiceGrpc.stub(channel)))
  }.unzip

  private val partitionClients = grpcClients.toList
    .sortBy { case (partitionAndReplica, _) => (partitionAndReplica.partitionNum, partitionAndReplica.replicaNum) }
    .foldLeft(Map.empty[Int, Seq[IndexServiceGrpc.IndexServiceStub]]) {
      case (acc, (PartitionAndReplica(partitionNum, _), client)) =>
        val old = acc.getOrElse(partitionNum, Seq.empty[IndexServiceGrpc.IndexServiceStub])
        acc.updated(partitionNum, old :+ client)
    }

  private val allPartitions = indexAddresses.map(_._1.partitionNum).toSeq.distinct: Seq[Int] // TODO not very nice

  private val threadPool = Executors.newFixedThreadPool(1)

  def search(
      vectorColumn: String,
      queryPartitionsColumn: Option[String],
      batch: Seq[Row],
      k: Int
  ): Iterator[Row] = {

    val queries = batch.map { row =>
      val partitions = queryPartitionsColumn.fold(allPartitions) { name => row.getAs[Seq[Int]](name) }
      val vector     = vectorConverter(vectorColumn, row)
      (partitions, vector)
    }

    // TODO should i use a random client or a client
    val randomClient = partitionClients.map { case (_, clients) => clients(random.nextInt(clients.size)) }

    val (requestObservers, responseIterators) = randomClient.zipWithIndex.toArray.map { case (client, partition) =>
      // TODO this is kind of inefficient
      val partitionCount = queries.count { case (partitions, _) => partitions.contains(partition) }

      val responseStreamObserver = new StreamObserverAdapter[SearchResponse](partitionCount)
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
          if (queryPartitions.contains(observerPartition)) {
            val request = SearchRequest(vector, k)
            observer.onNext(request)
          }
          if (last) {
            observer.onCompleted()
          }
        }
      }
    })

    val expectations = batch.zip(queries).map { case (row, (partitions, _)) => partitions -> row }.iterator

    new ResultsIterator(expectations, responseIterators: Array[Iterator[SearchResponse]], k)
  }

  def saveIndex(path: String): Unit = {
    val futures = partitionClients.flatMap { case (partition, clients) =>
      // only the primary replica saves the index
      clients.headOption.map { client =>
        val request = SaveIndexRequest(s"$path/$partition")
        client.saveIndex(request)
      }
    }

    Await.result(Future.sequence(futures), Duration.Inf) // TODO not sure if inf is smart
  }

  def shutdown(): Unit = {
    channels.foreach(_.shutdown())
    threadPool.shutdown()
  }

  private class StreamObserverAdapter[T](expected: Int) extends StreamObserver[T] with Iterator[T] {

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

    override def hasNext: Boolean = {
      !queue.isEmpty || (counter.get() < expected && !done.get())
    }

    override def next(): T = queue.take() match {
      case Right(value) => value
      case Left(t)      => throw t
    }
  }

  private class ResultsIterator(
      iterator: Iterator[(Seq[Int], Row)],
      partitionIterators: Array[Iterator[SearchResponse]],
      k: Int
  ) extends Iterator[Row] {

    override def hasNext: Boolean = iterator.hasNext

    override def next(): Row = {
      val (partitions, row) = iterator.next()

      val responses = partitions.map(partitionIterators.apply).map(_.next())

      val allResults = for {
        response <- responses
        result   <- response.results
      } yield idExtractor(result) -> distanceExtractor(result)

      val results = topK(allResults, k).map { case (id, distance) => Row(id, distance) }

      val n      = row.length
      val values = Array.ofDim[Any](n + 1)
      var i      = 0
      while (i < n) {
        values.update(i, row.get(i))
        i += 1
      }
      values.update(i, results)

      Row.fromSeq(values.toIndexedSeq)
    }
  }

  private def topK(allResults: Seq[(TId, TDistance)], k: Int): Seq[(TId, TDistance)] = {
    val pq = mutable.PriorityQueue[(TId, TDistance)]()(distanceOrdering.on[(TId, TDistance)](_._2).reverse)
    for ((id, distance) <- allResults) {
      if (pq.size < k) pq.enqueue((id, distance))
      else if (distanceOrdering.lt(distance, pq.head._2)) {
        pq.dequeue()
        pq.enqueue((id, distance))
      }
    }
    pq.dequeueAll
  }
}

class IndexClientFactory[TId, TVector, TDistance](
    vectorConverter: (String, Row) => SearchRequest.Vector,
    idExtractor: Result => TId,
    distanceExtractor: Result => TDistance
)(implicit distanceOrdering: Ordering[TDistance])
    extends Serializable {

  def create(servers: Map[PartitionAndReplica, InetSocketAddress]): IndexClient[TId, TVector, TDistance] =
    new IndexClient(servers, vectorConverter, idExtractor, distanceExtractor)

}
