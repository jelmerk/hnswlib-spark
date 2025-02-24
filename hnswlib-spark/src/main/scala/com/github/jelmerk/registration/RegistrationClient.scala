package com.github.jelmerk.registration

import java.net.{InetSocketAddress, SocketAddress}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import io.grpc.netty.NettyChannelBuilder

object RegistrationClient {

  def register(
      server: SocketAddress,
      partitionNo: Int,
      replicaNo: Int,
      indexServerAddress: InetSocketAddress
  ): RegisterResponse = {
    val channel = NettyChannelBuilder
      .forAddress(server)
      .usePlaintext
      .build()

    try {
      val client = RegistrationServiceGrpc.stub(channel)

      val request = RegisterRequest(
        partitionNum = partitionNo,
        replicaNum = replicaNo,
        indexServerAddress.getHostName,
        indexServerAddress.getPort
      )

      val response = client.register(request)

      Await.result(response, Duration.Inf)
    } finally {
      channel.shutdownNow()
    }

  }
}
