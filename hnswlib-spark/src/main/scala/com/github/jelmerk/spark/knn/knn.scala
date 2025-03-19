package com.github.jelmerk.spark

import java.io.{ObjectInput, ObjectOutput}

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.util.Try

import com.github.jelmerk.hnswlib.jdk17.Jdk17DistanceFunctions
import com.github.jelmerk.hnswlib.scala.{
  doubleBrayCurtisDistance,
  doubleCanberraDistance,
  doubleCorrelationDistance,
  doubleCosineDistance,
  doubleEuclideanDistance,
  doubleInnerProduct,
  doubleManhattanDistance,
  floatBrayCurtisDistance,
  floatCanberraDistance,
  floatCorrelationDistance,
  floatCosineDistance,
  floatEuclideanDistance,
  floatInnerProduct,
  floatManhattanDistance,
  DistanceFunction,
  Item,
  ObjectSerializer
}
import com.github.jelmerk.hnswlib.scala.jdk17DistanceFunctions.{
  vectorFloat128BrayCurtisDistance,
  vectorFloat128CanberraDistance,
  vectorFloat128CosineDistance,
  vectorFloat128EuclideanDistance,
  vectorFloat128InnerProduct,
  vectorFloat128ManhattanDistance
}
import com.github.jelmerk.index.{
  DenseVector,
  Distance,
  DoubleArrayVector,
  FloatArrayVector,
  Id => GrpcId,
  IndexClientFactory,
  IndexServerFactory,
  SparseVector,
  Vector => GrpcVector
}
import com.github.jelmerk.spark.linalg.functions.VectorDistanceFunctions
import org.apache.spark.ml.linalg.{DenseVector => SparkDenseVector, SparseVector => SparkSparseVector, Vector, Vectors}
import org.apache.spark.sql.Row

package object knn {

  private[knn] def typeDescription[T: TypeTag] = typeOf[T] match {
    case t if t =:= typeOf[Int]           => "int"
    case t if t =:= typeOf[Long]          => "long"
    case t if t =:= typeOf[String]        => "string"
    case t if t =:= typeOf[Array[Float]]  => "float_array"
    case t if t =:= typeOf[Array[Double]] => "double_array"
    case t if t =:= typeOf[Vector]        => "vector"
    case _                                => "unknown"
  }

  final private[knn] case class IntDoubleArrayIndexItem(id: Int, vector: Array[Double])
      extends Item[Int, Array[Double]] {
    override def dimensions: Int = vector.length
  }

  final private[knn] case class LongDoubleArrayIndexItem(id: Long, vector: Array[Double])
      extends Item[Long, Array[Double]] {
    override def dimensions: Int = vector.length
  }

  final private[knn] case class StringDoubleArrayIndexItem(id: String, vector: Array[Double])
      extends Item[String, Array[Double]] {
    override def dimensions: Int = vector.length
  }

  final private[knn] case class IntFloatArrayIndexItem(id: Int, vector: Array[Float]) extends Item[Int, Array[Float]] {
    override def dimensions: Int = vector.length
  }

  final private[knn] case class LongFloatArrayIndexItem(id: Long, vector: Array[Float])
      extends Item[Long, Array[Float]] {
    override def dimensions: Int = vector.length
  }

  final private[knn] case class StringFloatArrayIndexItem(id: String, vector: Array[Float])
      extends Item[String, Array[Float]] {
    override def dimensions: Int = vector.length
  }

  final private[knn] case class IntVectorIndexItem(id: Int, vector: Vector) extends Item[Int, Vector] {
    override def dimensions: Int = vector.size
  }

  final private[knn] case class LongVectorIndexItem(id: Long, vector: Vector) extends Item[Long, Vector] {
    override def dimensions: Int = vector.size
  }

  final private[knn] case class StringVectorIndexItem(id: String, vector: Vector) extends Item[String, Vector] {
    override def dimensions: Int = vector.size
  }

  private[knn] implicit object StringSerializer extends ObjectSerializer[String] {
    override def write(item: String, out: ObjectOutput): Unit = out.writeUTF(item)
    override def read(in: ObjectInput): String                = in.readUTF()
  }

  private[knn] implicit object IntSerializer extends ObjectSerializer[Int] {
    override def write(item: Int, out: ObjectOutput): Unit = out.writeInt(item)
    override def read(in: ObjectInput): Int                = in.readInt()
  }

  private[knn] implicit object LongSerializer extends ObjectSerializer[Long] {
    override def write(item: Long, out: ObjectOutput): Unit = out.writeLong(item)
    override def read(in: ObjectInput): Long                = in.readLong()
  }

  private[knn] implicit object FloatArraySerializer extends ObjectSerializer[Array[Float]] {
    override def write(item: Array[Float], out: ObjectOutput): Unit = {
      out.writeInt(item.length)
      item.foreach(out.writeFloat)
    }

    override def read(in: ObjectInput): Array[Float] = {
      val length = in.readInt()
      val item   = Array.ofDim[Float](length)

      for (i <- 0 until length) {
        item(i) = in.readFloat()
      }
      item
    }
  }

  private[knn] implicit object DoubleArraySerializer extends ObjectSerializer[Array[Double]] {
    override def write(item: Array[Double], out: ObjectOutput): Unit = {
      out.writeInt(item.length)
      item.foreach(out.writeDouble)
    }

    override def read(in: ObjectInput): Array[Double] = {
      val length = in.readInt()
      val item   = Array.ofDim[Double](length)

      for (i <- 0 until length) {
        item(i) = in.readDouble()
      }
      item
    }
  }

  private[knn] implicit object VectorSerializer extends ObjectSerializer[Vector] {
    override def write(item: Vector, out: ObjectOutput): Unit = item match {
      case v: SparkDenseVector =>
        out.writeBoolean(true)
        out.writeInt(v.size)
        v.values.foreach(out.writeDouble)

      case v: SparkSparseVector =>
        out.writeBoolean(false)
        out.writeInt(v.size)
        out.writeInt(v.indices.length)
        v.indices.foreach(out.writeInt)
        v.values.foreach(out.writeDouble)
    }

    override def read(in: ObjectInput): Vector = {
      val isDense = in.readBoolean()
      val size    = in.readInt()

      if (isDense) {
        val values = Array.ofDim[Double](size)

        for (i <- 0 until size) {
          values(i) = in.readDouble()
        }

        Vectors.dense(values)
      } else {
        val numFilled = in.readInt()
        val indices   = Array.ofDim[Int](numFilled)

        for (i <- 0 until numFilled) {
          indices(i) = in.readInt()
        }

        val values = Array.ofDim[Double](numFilled)

        for (i <- 0 until numFilled) {
          values(i) = in.readDouble()
        }

        Vectors.sparse(size, indices, values)
      }
    }
  }

  private[knn] implicit object IntVectorIndexItemSerializer extends ObjectSerializer[IntVectorIndexItem] {
    override def write(item: IntVectorIndexItem, out: ObjectOutput): Unit = {
      IntSerializer.write(item.id, out)
      VectorSerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): IntVectorIndexItem = {
      val id     = IntSerializer.read(in)
      val vector = VectorSerializer.read(in)
      IntVectorIndexItem(id, vector)
    }
  }

  private[knn] implicit object LongVectorIndexItemSerializer extends ObjectSerializer[LongVectorIndexItem] {
    override def write(item: LongVectorIndexItem, out: ObjectOutput): Unit = {
      LongSerializer.write(item.id, out)
      VectorSerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): LongVectorIndexItem = {
      val id     = LongSerializer.read(in)
      val vector = VectorSerializer.read(in)
      LongVectorIndexItem(id, vector)
    }
  }

  private[knn] implicit object StringVectorIndexItemSerializer extends ObjectSerializer[StringVectorIndexItem] {
    override def write(item: StringVectorIndexItem, out: ObjectOutput): Unit = {
      StringSerializer.write(item.id, out)
      VectorSerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): StringVectorIndexItem = {
      val id     = StringSerializer.read(in)
      val vector = VectorSerializer.read(in)
      StringVectorIndexItem(id, vector)
    }
  }

  private[knn] implicit object IntFloatArrayIndexItemSerializer extends ObjectSerializer[IntFloatArrayIndexItem] {
    override def write(item: IntFloatArrayIndexItem, out: ObjectOutput): Unit = {
      IntSerializer.write(item.id, out)
      FloatArraySerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): IntFloatArrayIndexItem = {
      val id     = IntSerializer.read(in)
      val vector = FloatArraySerializer.read(in)
      IntFloatArrayIndexItem(id, vector)
    }
  }

  private[knn] implicit object LongFloatArrayIndexItemSerializer extends ObjectSerializer[LongFloatArrayIndexItem] {
    override def write(item: LongFloatArrayIndexItem, out: ObjectOutput): Unit = {
      LongSerializer.write(item.id, out)
      FloatArraySerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): LongFloatArrayIndexItem = {
      val id     = LongSerializer.read(in)
      val vector = FloatArraySerializer.read(in)
      LongFloatArrayIndexItem(id, vector)
    }
  }

  private[knn] implicit object StringFloatArrayIndexItemSerializer extends ObjectSerializer[StringFloatArrayIndexItem] {
    override def write(item: StringFloatArrayIndexItem, out: ObjectOutput): Unit = {
      StringSerializer.write(item.id, out)
      FloatArraySerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): StringFloatArrayIndexItem = {
      val id     = StringSerializer.read(in)
      val vector = FloatArraySerializer.read(in)
      StringFloatArrayIndexItem(id, vector)
    }
  }

  private[knn] implicit object IntDoubleArrayIndexItemSerializer extends ObjectSerializer[IntDoubleArrayIndexItem] {
    override def write(item: IntDoubleArrayIndexItem, out: ObjectOutput): Unit = {
      IntSerializer.write(item.id, out)
      DoubleArraySerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): IntDoubleArrayIndexItem = {
      val id     = IntSerializer.read(in)
      val vector = DoubleArraySerializer.read(in)
      IntDoubleArrayIndexItem(id, vector)
    }
  }

  private[knn] implicit object LongDoubleArrayIndexItemSerializer extends ObjectSerializer[LongDoubleArrayIndexItem] {
    override def write(item: LongDoubleArrayIndexItem, out: ObjectOutput): Unit = {
      LongSerializer.write(item.id, out)
      DoubleArraySerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): LongDoubleArrayIndexItem = {
      val id     = LongSerializer.read(in)
      val vector = DoubleArraySerializer.read(in)
      LongDoubleArrayIndexItem(id, vector)
    }
  }

  private[knn] implicit object StringDoubleArrayIndexItemSerializer
      extends ObjectSerializer[StringDoubleArrayIndexItem] {
    override def write(item: StringDoubleArrayIndexItem, out: ObjectOutput): Unit = {
      StringSerializer.write(item.id, out)
      DoubleArraySerializer.write(item.vector, out)
    }

    override def read(in: ObjectInput): StringDoubleArrayIndexItem = {
      val id     = StringSerializer.read(in)
      val vector = DoubleArraySerializer.read(in)
      StringDoubleArrayIndexItem(id, vector)
    }
  }

  private[knn] implicit object IntVectorIndexServerFactory
      extends IndexServerFactory[Int, Vector, IntVectorIndexItem, Double](
        VectorCodec,
        IntIdCodec,
        DoubleDistance
      )

  private[knn] implicit object LongVectorIndexServerFactory
      extends IndexServerFactory[Long, Vector, LongVectorIndexItem, Double](
        VectorCodec,
        LongIdCodec,
        DoubleDistance
      )

  private[knn] implicit object StringVectorIndexServerFactory
      extends IndexServerFactory[String, Vector, StringVectorIndexItem, Double](
        VectorCodec,
        StringIdCodec,
        DoubleDistance
      )

  private[knn] implicit object IntFloatArrayIndexServerFactory
      extends IndexServerFactory[Int, Array[Float], IntFloatArrayIndexItem, Float](
        FloatArrayCodec,
        IntIdCodec,
        FloatDistance
      )

  private[knn] implicit object LongFloatArrayIndexServerFactory
      extends IndexServerFactory[Long, Array[Float], LongFloatArrayIndexItem, Float](
        FloatArrayCodec,
        LongIdCodec,
        FloatDistance
      )

  private[knn] implicit object StringFloatArrayIndexServerFactory
      extends IndexServerFactory[String, Array[Float], StringFloatArrayIndexItem, Float](
        FloatArrayCodec,
        StringIdCodec,
        FloatDistance
      )

  private[knn] implicit object IntDoubleArrayIndexServerFactory
      extends IndexServerFactory[Int, Array[Double], IntDoubleArrayIndexItem, Double](
        DoubleArrayCodec,
        IntIdCodec,
        DoubleDistance
      )

  private[knn] implicit object LongDoubleArrayIndexServerFactory
      extends IndexServerFactory[Long, Array[Double], LongDoubleArrayIndexItem, Double](
        DoubleArrayCodec,
        LongIdCodec,
        DoubleDistance
      )

  private[knn] implicit object StringDoubleArrayIndexServerFactory
      extends IndexServerFactory[String, Array[Double], StringDoubleArrayIndexItem, Double](
        DoubleArrayCodec,
        StringIdCodec,
        DoubleDistance
      )

  private[knn] implicit object IntVectorIndexClientFactory
      extends IndexClientFactory[Int, Vector, Double](
        convertVector,
        IntIdCodec,
        DoubleDistance
      )

  private[knn] implicit object LongVectorIndexClientFactory
      extends IndexClientFactory[Long, Vector, Double](
        convertVector,
        LongIdCodec,
        DoubleDistance
      )

  private[knn] implicit object StringVectorIndexClientFactory
      extends IndexClientFactory[String, Vector, Double](
        convertVector,
        StringIdCodec,
        DoubleDistance
      )

  private[knn] implicit object IntFloatArrayIndexClientFactory
      extends IndexClientFactory[Int, Array[Float], Float](
        convertFloatArray,
        IntIdCodec,
        FloatDistance
      )

  private[knn] implicit object LongFloatArrayIndexClientFactory
      extends IndexClientFactory[Long, Array[Float], Float](
        convertFloatArray,
        LongIdCodec,
        FloatDistance
      )

  private[knn] implicit object StringFloatArrayIndexClientFactory
      extends IndexClientFactory[String, Array[Float], Float](
        convertFloatArray,
        StringIdCodec,
        FloatDistance
      )

  private[knn] implicit object IntDoubleArrayIndexClientFactory
      extends IndexClientFactory[Int, Array[Double], Double](
        convertDoubleArray,
        IntIdCodec,
        DoubleDistance
      )

  private[knn] implicit object LongDoubleArrayIndexClientFactory
      extends IndexClientFactory[Long, Array[Double], Double](
        convertDoubleArray,
        LongIdCodec,
        DoubleDistance
      )

  private[knn] implicit object StringDoubleArrayIndexClientFactory
      extends IndexClientFactory[String, Array[Double], Double](
        convertDoubleArray,
        StringIdCodec,
        DoubleDistance
      )

  private[knn] def convertFloatArray(column: String, row: Row): GrpcVector =
    GrpcVector(GrpcVector.Value.FloatArrayVector(FloatArrayVector(row.getAs[Seq[Float]](column).toArray)))

  private[knn] def convertDoubleArray(column: String, row: Row): GrpcVector =
    GrpcVector(GrpcVector.Value.DoubleArrayVector(DoubleArrayVector(row.getAs[Seq[Double]](column).toArray)))

  private[knn] def convertVector(column: String, row: Row): GrpcVector = row.getAs[Vector](column) match {
    case v: SparkDenseVector  => GrpcVector(GrpcVector.Value.DenseVector(DenseVector(v.values)))
    case v: SparkSparseVector => GrpcVector(GrpcVector.Value.SparseVector(SparseVector(v.size, v.indices, v.values)))
  }

  private[jelmerk] trait Codec[A, B] extends Serializable {

    def encode(a: A): B

    def decode(b: B): A

  }

  private[knn] object StringIdCodec extends Codec[String, GrpcId] {
    override def encode(a: String): GrpcId = GrpcId(GrpcId.Value.StringId(a))

    override def decode(b: GrpcId): String = b.getStringId
  }

  private[knn] object LongIdCodec extends Codec[Long, GrpcId] {
    override def encode(a: Long): GrpcId = GrpcId(GrpcId.Value.LongId(a))

    override def decode(b: GrpcId): Long = b.getLongId
  }

  private[knn] object IntIdCodec extends Codec[Int, GrpcId] {
    override def encode(a: Int): GrpcId = GrpcId(GrpcId.Value.IntId(a))

    override def decode(b: GrpcId): Int = b.getIntId
  }

  private[knn] object FloatArrayCodec extends Codec[Array[Float], GrpcVector] {

    override def encode(a: Array[Float]): GrpcVector =
      GrpcVector(GrpcVector.Value.FloatArrayVector(FloatArrayVector(a)))

    override def decode(b: GrpcVector): Array[Float] = b.getFloatArrayVector.values
  }

  private[knn] object DoubleArrayCodec extends Codec[Array[Double], GrpcVector] {

    override def encode(a: Array[Double]): GrpcVector =
      GrpcVector(GrpcVector.Value.DoubleArrayVector(DoubleArrayVector(a)))

    override def decode(b: GrpcVector): Array[Double] = b.getDoubleArrayVector.values
  }

  private[knn] object VectorCodec extends Codec[Vector, GrpcVector] {

    override def encode(a: Vector): GrpcVector = a match {
      case v: SparkDenseVector  => GrpcVector(GrpcVector.Value.DenseVector(DenseVector(v.values)))
      case v: SparkSparseVector => GrpcVector(GrpcVector.Value.SparseVector(SparseVector(v.size, v.indices, v.values)))
    }

    override def decode(b: GrpcVector): Vector =
      if (b.value.isDenseVector) new SparkDenseVector(b.getDenseVector.values)
      else {
        val v = b.getSparseVector
        new SparkSparseVector(v.size, v.indices, v.values)
      }
  }

  private[knn] object FloatDistance extends Codec[Float, Distance] {
    override def encode(a: Float): Distance = Distance(Distance.Value.FloatDistance(a))

    override def decode(b: Distance): Float = b.getFloatDistance
  }

  private[knn] object DoubleDistance extends Codec[Double, Distance] {
    override def encode(a: Double): Distance = Distance(Distance.Value.DoubleDistance(a))

    override def decode(b: Distance): Double = b.getDoubleDistance
  }

  implicit private[knn] def floatArrayDistanceFunction(name: String): DistanceFunction[Array[Float], Float] =
    (name, vectorApiAvailable) match {
      case ("bray-curtis", true)   => vectorFloat128BrayCurtisDistance
      case ("bray-curtis", _)      => floatBrayCurtisDistance
      case ("canberra", true)      => vectorFloat128CanberraDistance
      case ("canberra", _)         => floatCanberraDistance
      case ("correlation", _)      => floatCorrelationDistance
      case ("cosine", true)        => vectorFloat128CosineDistance
      case ("cosine", _)           => floatCosineDistance
      case ("euclidean", true)     => vectorFloat128EuclideanDistance
      case ("euclidean", _)        => floatEuclideanDistance
      case ("inner-product", true) => vectorFloat128InnerProduct
      case ("inner-product", _)    => floatInnerProduct
      case ("manhattan", true)     => vectorFloat128ManhattanDistance
      case ("manhattan", _)        => floatManhattanDistance
      case (value, _)              => userDistanceFunction(value)
    }

  implicit private[knn] def doubleArrayDistanceFunction(name: String): DistanceFunction[Array[Double], Double] =
    name match {
      case "bray-curtis"   => doubleBrayCurtisDistance
      case "canberra"      => doubleCanberraDistance
      case "correlation"   => doubleCorrelationDistance
      case "cosine"        => doubleCosineDistance
      case "euclidean"     => doubleEuclideanDistance
      case "inner-product" => doubleInnerProduct
      case "manhattan"     => doubleManhattanDistance
      case value           => userDistanceFunction(value)
    }

  implicit private[knn] def vectorDistanceFunction(name: String): DistanceFunction[Vector, Double] = name match {
    case "bray-curtis"   => VectorDistanceFunctions.brayCurtisDistance
    case "canberra"      => VectorDistanceFunctions.canberraDistance
    case "correlation"   => VectorDistanceFunctions.correlationDistance
    case "cosine"        => VectorDistanceFunctions.cosineDistance
    case "euclidean"     => VectorDistanceFunctions.euclideanDistance
    case "inner-product" => VectorDistanceFunctions.innerProduct
    case "manhattan"     => VectorDistanceFunctions.manhattanDistance
    case value           => userDistanceFunction(value)
  }

  @SuppressWarnings(Array("CatchThrowable"))
  private def vectorApiAvailable: Boolean = try {
    val _ = Jdk17DistanceFunctions.VECTOR_FLOAT_128_COSINE_DISTANCE
    true
  } catch {
    case _: Throwable => false
  }

  private def userDistanceFunction[TVector, TDistance](name: String): DistanceFunction[TVector, TDistance] =
    Try(Class.forName(name).getDeclaredConstructor().newInstance()).toOption
      .collect { case f: DistanceFunction[TVector @unchecked, TDistance @unchecked] => f }
      .getOrElse(throw new IllegalArgumentException(s"$name is not a valid distance functions."))
}
