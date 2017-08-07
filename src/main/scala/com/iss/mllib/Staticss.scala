package com.iss.mllib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by hadoop on 2017/8/8 0008.
  */
object Staticss {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("OneHotEncoderExample").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    //spark.createDataFrame()
//    val observations: RDD[Vector] = Vectors.dense(1.0,0.3,0.4,0.5,0.6)//  Vectors RDD
//
//    // 计算列统计量
//    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
//    println(summary.mean) // 均值
//    println(summary.variance) // 方差
//    println(summary.numNonzeros) // 列非零值
  }
}
