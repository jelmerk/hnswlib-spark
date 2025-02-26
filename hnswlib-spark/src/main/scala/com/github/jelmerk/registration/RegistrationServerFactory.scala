package com.github.jelmerk.registration

import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

import com.github.jelmerk.registration.RegistrationServiceGrpc.RegistrationService
import io.grpc.netty.NettyServerBuilder

class DefaultRegistrationService(val registrationLatch: CountDownLatch) extends RegistrationService {

  val registrations = new ConcurrentHashMap[PartitionAndReplica, InetSocketAddress]()

  override def register(request: RegisterRequest): Future[RegisterResponse] = {

    val key           = PartitionAndReplica(request.partitionNum, request.replicaNum)
    val previousValue = Option(registrations.put(key, new InetSocketAddress(request.host, request.port)))

    if (previousValue.isEmpty) {
      registrationLatch.countDown()
    }

    Future.successful(RegisterResponse())
  }

}

final case class PartitionAndReplica(partitionNum: Int, replicaNum: Int)

class RegistrationServer(host: String, numPartitions: Int, numReplicas: Int) {

  private val executor = Executors.newSingleThreadExecutor()

  private val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private val registrationLatch = new CountDownLatch(numPartitions + (numReplicas * numPartitions))
  private val service           = new DefaultRegistrationService(registrationLatch)

  private val server = NettyServerBuilder
    .forAddress(new InetSocketAddress(host, 0))
    .addService(RegistrationServiceGrpc.bindService(service, executionContext))
    .build()

  def start(): InetSocketAddress = {
    server.start()
    server.getListenSockets.asScala.headOption
      .collect { case addr: InetSocketAddress =>
        addr
      }
      .getOrElse(throw new IllegalStateException)
  }

  def awaitRegistrations(): Map[PartitionAndReplica, InetSocketAddress] = {
    service.registrationLatch.await()
    service.registrations.asScala.toMap
  }

  def shutdown(): Unit = {
    Try(server.shutdown())
    Try(executor.shutdown())
  }

}

object RegistrationServerFactory {

  def create(host: String, numPartitions: Int, numReplicas: Int): RegistrationServer =
    new RegistrationServer(host, numPartitions, numReplicas)
}
