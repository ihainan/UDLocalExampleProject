package me.ihainan.test

import java.io.File
import java.net.URLClassLoader
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import scala.util.Try

object Application extends App {
  val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val customTransformerURL = new File("./transformer.jar").toURI.toURL
  val newClassLoader = new URLClassLoader(Array(customTransformerURL), this.getClass.getClassLoader)
  Thread.currentThread().setContextClassLoader(newClassLoader)
  val model = PipelineModel.load("./model")
  val df = spark.createDataFrame(Seq(
    (1, 2, "1"),
    (3, 4, "2"),
    (5, 6, "3")
  )).toDF("C1", "C2", "C3")
  Try(model.transform(df).show())
  Thread.sleep(500000)
}