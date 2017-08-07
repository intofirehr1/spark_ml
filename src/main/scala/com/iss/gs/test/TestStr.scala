package com.iss.gs.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by hadoop on 2017/8/11 0011.
  */
object TestStr {

  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("gs_train").setMaster(args(0))
//    // 设置运行参数： cpu, mem
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    val schemaString = ""
//    val colNames = schemaString.collect()
//    val features = colNames.array(colNames.array.size-1).split(",")
//    var featuresCols = ""
//    for( a <- 1 to features.array.size){
//      featuresCols ++ "FFEATURES" + a ++ ","
//    }
//    val userSchemaString = colNames.toString + "," + featuresCols.substring(0,featuresCols.length-1)
//    println(featuresCols)
  }
}
