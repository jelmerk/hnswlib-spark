package com.github.jelmerk.index

import java.net.InetSocketAddress
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

import com.github.jelmerk.index.IndexServiceGrpc.IndexService
import com.github.jelmerk.knn.scalalike.{Index, Item}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class DefaultIndexService[TId, TVector, TItem <: Item[TId, TVector], TDistance](
    index: Index[TId, TVector, TItem, TDistance],
    hadoopConfiguration: Configuration,
    vectorExtractor: SearchRequest => TVector,
    resultIdConverter: TId => Result.Id,
    resultDistanceConverter: TDistance => Result.Distance
)(implicit
    executionContext: ExecutionContext
) extends IndexService {

  override def search(responseObserver: StreamObserver[SearchResponse]): StreamObserver[SearchRequest] = {
    new StreamObserver[SearchRequest] {
      override def onNext(request: SearchRequest): Unit = {

        val vector  = vectorExtractor(request)
        val nearest = index.findNearest(vector, request.k)

        val results = nearest.map { searchResult =>
          val id       = resultIdConverter(searchResult.item.id)
          val distance = resultDistanceConverter(searchResult.distance)

          Result(id, distance)
        }

        val response = SearchResponse(results)
        responseObserver.onNext(response)
      }

      override def onError(t: Throwable): Unit = {
        responseObserver.onError(t)
      }

      override def onCompleted(): Unit = {
        responseObserver.onCompleted()
      }
    }
  }

  override def saveIndex(request: SaveIndexRequest): Future[SaveIndexResponse] = Future {
    val path       = new Path(request.path)
    val fileSystem = path.getFileSystem(hadoopConfiguration)

    val outputStream = fileSystem.create(path)

    val countingOutputStream = new CountingOutputStream(outputStream)

    index.save(countingOutputStream)

    SaveIndexResponse(bytesWritten = countingOutputStream.getByteCount)
  }

}

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

  def start(): InetSocketAddress = {
    server.start()
    server.getListenSockets.asScala.headOption
      .collect { case addr: InetSocketAddress =>
        addr
      }
      .getOrElse(throw new IllegalStateException)
  }

  def isTerminated: Boolean = {
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
