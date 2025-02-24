package com.github.jelmerk.registration

import java.net.InetSocketAddress
import java.util.concurrent.{CountDownLatch, Executors}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.Try

import io.grpc.netty.NettyServerBuilder

class RegistrationServer(host: String, numPartitions: Int, numReplicas: Int) {

  private val executor = Executors.newSingleThreadExecutor()

  private val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private val registrationLatch = new CountDownLatch(numPartitions + (numReplicas * numPartitions))
  private val service           = new DefaultRegistrationService(registrationLatch)

  private val server = NettyServerBuilder
    .forAddress(new InetSocketAddress(host, 0))
    .addService(RegistrationServiceGrpc.bindService(service, executionContext))
    .build()

  def start(): Unit = server.start()

  def address: InetSocketAddress = server.getListenSockets.get(0).asInstanceOf[InetSocketAddress] // TODO CLEANUP

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
