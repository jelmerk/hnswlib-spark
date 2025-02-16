package com.github.jelmerk.spark.util

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/** Like [[org.apache.spark.ml.util.DefaultParamsWritable]] but does not schedule work on the executors. */
trait DefaultParamsWritable extends MLWritable { self: Params =>

  override def write: MLWriter = new DefaultParamsWriter(this)

}

class DefaultParamsWriter(instance: Params) extends MLWriter {

  override protected def saveImpl(path: String): Unit = {
    saveMetadata(instance, path, sc)
  }

  private def saveMetadata(
      instance: Params,
      path: String,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None,
      paramMap: Option[JValue] = None
  ): Unit = {
    val metadataPath = new Path(path, "metadata")
    val metadataJson = getMetadataToSave(instance, sc, extraMetadata, paramMap)

    val fs = metadataPath.getFileSystem(sc.hadoopConfiguration)

    val jsonPath = new Path(metadataPath, "part-00000")
    val out      = fs.create(jsonPath)
    try {
      out.write(metadataJson.getBytes(StandardCharsets.UTF_8))
    } finally {
      out.close()
    }

    fs.create(new Path(metadataPath, "_SUCCESS")).close()
  }

  private def getMetadataToSave(
      instance: Params,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None,
      paramMap: Option[JValue] = None
  ): String = {
    val uid = instance.uid
    val cls = instance.getClass.getName

    val allParams               = instance.extractParamMap
    val (params, defaultParams) = allParams.toSeq.partition(p => instance.isDefined(p.param))
    val jsonParams = paramMap.getOrElse(render(params.map { case ParamPair(p, v) =>
      p.name -> parse(p.jsonEncode(v))
    }.toList))
    val jsonDefaultParams = render(defaultParams.map { case ParamPair(p, v) =>
      p.name -> parse(p.jsonEncode(v))
    }.toList)
    val basicMetadata = ("class" -> cls) ~
      ("timestamp"       -> System.currentTimeMillis()) ~
      ("sparkVersion"    -> sc.version) ~
      ("uid"             -> uid) ~
      ("paramMap"        -> jsonParams) ~
      ("defaultParamMap" -> jsonDefaultParams)

    val metadata = extraMetadata match {
      case Some(jObject) => basicMetadata ~ jObject
      case None          => basicMetadata
    }
    val metadataJson: String = compact(render(metadata))
    metadataJson
  }

}
