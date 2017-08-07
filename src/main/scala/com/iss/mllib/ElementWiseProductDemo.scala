package com.iss.mllib

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by hadoop on 2017/8/8 0008.
  */
object ElementWiseProductDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("naviebayes webclick")
      .setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // Create some vector data; also works for sparse vectors
    val dataFrame = spark.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct().setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show()
  }






}
