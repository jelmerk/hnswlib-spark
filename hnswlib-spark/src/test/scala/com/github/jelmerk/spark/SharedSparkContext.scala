package com.github.jelmerk.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var internalSparkSession: SparkSession = _

  def appID: String = this.getClass.getName + math.floor(math.random() * 10e4).toLong.toString

  def conf: SparkConf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
      .set("spark.barrier.sync.timeout", "10")
      .set("spark.stage.maxConsecutiveAttempts", "1")

  lazy val spark: SparkSession = internalSparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    internalSparkSession = SparkSession.builder().config(conf).getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      Thread.sleep(1000) // hack to avoid tons of exceptions during build
      Option(internalSparkSession).foreach { _.stop() }
      // To avoid Akka rebinding to the same port, since it doesn't
      // unbind immediately on shutdown.
      System.clearProperty("spark.driver.port")
      internalSparkSession = null
    } finally {
      super.afterAll()
    }
  }

}
