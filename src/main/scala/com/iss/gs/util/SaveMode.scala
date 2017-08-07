package com.iss.gs.util

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by hadoop on 2017/8/8 0008.
  */
object MySaveMode {

  def saveMode(dataFrame: DataFrame, savePath : String, formater : String ): Unit ={
    val saveOptions = Map("header" -> "true",
      "path" -> savePath)  //"iteblog.csv"
    //Built-in options include "parquet", "json", "csv", "txt"
    dataFrame.write.format(formater).mode(SaveMode.Overwrite).options(saveOptions).save()
  }

}
