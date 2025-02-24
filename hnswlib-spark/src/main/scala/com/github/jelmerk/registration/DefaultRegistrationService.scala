package com.github.jelmerk.registration

import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import scala.concurrent.Future

import com.github.jelmerk.registration.RegistrationServiceGrpc.RegistrationService

class DefaultRegistrationService(val registrationLatch: CountDownLatch) extends RegistrationService {

  val registrations = new ConcurrentHashMap[PartitionAndReplica, InetSocketAddress]()

  override def register(request: RegisterRequest): Future[RegisterResponse] = {

    val key           = PartitionAndReplica(request.partitionNum, request.replicaNum)
    val previousValue = registrations.put(key, new InetSocketAddress(request.host, request.port))

    if (previousValue == null) {
      registrationLatch.countDown()
    }

    Future.successful(RegisterResponse())
  }

}

final case class PartitionAndReplica(partitionNum: Int, replicaNum: Int)
