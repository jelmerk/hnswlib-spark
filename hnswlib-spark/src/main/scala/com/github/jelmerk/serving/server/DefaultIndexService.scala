package com.github.jelmerk.serving.server

import scala.concurrent.{ExecutionContext, Future}

import com.github.jelmerk.knn.scalalike.{Index, Item}
import com.github.jelmerk.server.index._
import com.github.jelmerk.server.index.IndexServiceGrpc.IndexService
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
