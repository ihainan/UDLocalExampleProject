package me.ihainan.test

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class MyTransformer(override val uid: String)
  extends Transformer
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("MyTransformer"))

  override def copy(extra: org.apache.spark.ml.param.ParamMap): MyTransformer = {
    new MyTransformer()
  }

  override def transform(df: Dataset[_]): org.apache.spark.sql.DataFrame = {
    import df.sparkSession.implicits._

    val addOneUDF = udf { in: Int =>
      in + 1
    }

    df.groupBy("C3").agg(addOneUDF(sum($"C2")).as("text_counts"))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField("text_counts", IntegerType, true))
  }
}

object MyTransformer extends DefaultParamsReadable[MyTransformer] {}
