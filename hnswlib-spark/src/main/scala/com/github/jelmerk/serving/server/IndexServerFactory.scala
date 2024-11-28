package com.github.jelmerk.serving.server

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.ExecutionContext
import scala.util.Try

import com.github.jelmerk.knn.scalalike.{Index, Item}
import com.github.jelmerk.server.index.{IndexServiceGrpc, Result, SearchRequest}
import io.grpc.netty.NettyServerBuilder
import org.apache.hadoop.conf.Configuration

class IndexServer[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance](
    host: String,
    vectorExtractor: SearchRequest => TVector,
    resultIdConverter: TId => Result.Id,
    resultDistanceConverter: TDistance => Result.Distance,
    index: Index[TId, TVector, TItem, TDistance],
    hadoopConfig: Configuration,
    threads: Int
) {
  private val executor = new ThreadPoolExecutor(
    threads,
    threads,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable]()
  )

  private val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val service =
    new DefaultIndexService(index, hadoopConfig, vectorExtractor, resultIdConverter, resultDistanceConverter)

  // Build the gRPC server
  private val server = NettyServerBuilder
    .forAddress(new InetSocketAddress(host, 0))
    .addService(IndexServiceGrpc.bindService(service, executionContext))
    .build()

  def start(): Unit = server.start()

  def address: InetSocketAddress = server.getListenSockets.get(0).asInstanceOf[InetSocketAddress] // TODO CLEANUP

  def awaitTermination(): Unit = {
    server.awaitTermination()
  }

  def isTerminated(): Boolean = {
    server.isTerminated
  }

  def shutdown(): Unit = {
    Try(server.shutdown())
    Try(executor.shutdown())
  }

  def shutdownNow(): Unit = {
    Try(server.shutdownNow())
    Try(executor.shutdownNow())
  }
}

class IndexServerFactory[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance](
    vectorExtractor: SearchRequest => TVector,
    resultIdConverter: TId => Result.Id,
    resultDistanceConverter: TDistance => Result.Distance
) extends Serializable {

  def create(
      host: String,
      index: Index[TId, TVector, TItem, TDistance],
      hadoopConfig: Configuration,
      threads: Int
  ): IndexServer[TId, TVector, TItem, TDistance] = {
    new IndexServer[TId, TVector, TItem, TDistance](
      host,
      vectorExtractor,
      resultIdConverter,
      resultDistanceConverter,
      index,
      hadoopConfig,
      threads
    )

  }
}
