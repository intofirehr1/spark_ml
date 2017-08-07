package com.iss.gs.util

import java.util

import org.apache.spark.sql.{Row, SparkSession, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by hadoop on 2017/8/7 0007.
  */
object Utils {

  def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }

  def isColumnNameLine1(line:String):Boolean = {
    if (line != null &&
      line.contains("feature_col_index")) true
    else false
  }

  def saveResult(spark: SparkSession, dataDF : DataFrame, label2UserList: util.ArrayList[Row],
                 resultSavePath: String, formater : String): Unit ={
    val labelTable = "TRANS_CODE,TRANS_BAT_NO,PREDICT_LABEL";
    val labelSchemaFeatures = StructType(labelTable.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val label2UserDF = spark.createDataFrame(label2UserList, labelSchemaFeatures)
    label2UserDF.show(10)
    println("===========")
    val predictResult = dataDF.join(label2UserDF, Seq("TRANS_CODE","TRANS_BAT_NO"), "left")

    predictResult.show(10)

    MySaveMode.saveMode(predictResult, resultSavePath, formater)

  }

}

class Utils{

}


case class Label2User(trans_code : String, trans_bat_no : String, label : Double)

