package com.iss.gs.test

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2017/10/15 0015.
  */
object KmeansTest {
  def main(args: Array[String]): Unit = {
    if(args.size < 2){
      System.err.println("params: master datapath testDataPath")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("gs_bisectkmeans_train").setMaster(args(0))
    // 设置运行参数： cpu, mem
    val sc = new SparkContext(conf)
    //val datas = sc.textFile(args(1)).filter(!isColumnNameLine(_)).map(parse)
    //val srcDatas = sc.textFile(args(1))

    val datas = sc.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))

    val k = 3

    // 类别处理 系统编号 地址，学历，险种需要处理类别
    val sysNo = datas.map(r => r(1)).distinct().collect().zipWithIndex.toMap
    val numSysNo = sysNo.size

    val lrData = datas.map { line =>

      // 系统编号处理
      val sysNoIdx = sysNo(line(1))
      val sysNoFeatures = Array.ofDim[Double](numSysNo)
      sysNoFeatures(sysNoIdx) = 1.0

      //性别
      val userSexFeatures = line.slice(3,4).map( r =>
        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3.0
      )
      val wholeFeatures = userSexFeatures ++ sysNoFeatures
//        userSexFeatures ++ mioClassFeatures  ++
//        sysNoFeatures ++
//        addressNoFeatures ++ educationNoFeatures ++ insuranceTypeNoFeatures ++
//        transClassFeatures ++ userMaritalFeatures ++ otherFeatures

      Vectors.dense(wholeFeatures)
    }


//    val bkm = new BisectingKMeans().setK(k)  // 6
//    val model = bkmrun(lrData)
//    println(s"Compute Cost: ${model.computeCost(lrData)}")
//    model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
//      println(s"Cluster Center ${idx}: ${center}")
//        println()
//    }

  }



  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }
}
