package com.iss.gs.test

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2017/8/8 0008.
  */
object NaiveBayes_GS_Tags {

  def main(args: Array[String]): Unit = {
    if(args.size < 4){
      System.err.println("params: master datapath testDataPath modelsavepath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_train").setMaster(args(0))
    // 设置运行参数： cpu, mem

    val sc = new SparkContext(conf)

    val datas = sc.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))

    // 数据清理 TODO

    // 类别处理 系统编号 地址，学历，险种需要处理类别
    val sysNo = datas.map(r => r(1)).distinct().collect().zipWithIndex.toMap
    val numSysNo = sysNo.size

    // 险种
    val insuranceTypeNo = datas.map(r => r(48)).distinct().collect().zipWithIndex.toMap
    val numInsuranceTypeNo = insuranceTypeNo.size

    val nbData = datas.map { line =>
      //  TODO 特殊数据处理，如：负数字段处理, 字母处理
      val label = line(4).toInt
//       val label = if(line(4).toInt ==1 ) 1 else 0

      // 系统编号处理
      val sysNoIdx = sysNo(line(1))
      val sysNoFeatures = Array.ofDim[Double](numSysNo)
      sysNoFeatures(sysNoIdx) = 1.0

      // 转账类型处理
      val transClassFeatures = line.slice(21,22).map(r => if(r.equals("F")) 0.0 else 1.0)
      // 付款类型处理
      val mioClassFeatures = line.slice(22,23).map(r => if(r.toDouble < 0) 0.0 else r.toDouble)
      // 金额
      // val transAmntFeatures = line.slice(26,27).map(r => if(!r.equals("") && r.toDouble>0) r.toDouble else 0.0)

      // 险种处理
      val insuranceTypeNoIdx = insuranceTypeNo(line(48))
      val insuranceTypeNoFeatures = Array.ofDim[Double](numInsuranceTypeNo)
      insuranceTypeNoFeatures(insuranceTypeNoIdx) = 1.0

      //客户价值，对保险的认知程度，渠道
      val otherFeatures = line.slice(50,52).map( r =>
        if(!r.equals("") && r.toDouble >= 0) r.toDouble else 0.0
      )

      // 标签
      val tags = line.slice(53,line.size-1).map(_.toDouble)
      //tags.foreach(println)

      val wholeFeatures =  mioClassFeatures  ++
        sysNoFeatures ++ insuranceTypeNoFeatures ++
        transClassFeatures ++ otherFeatures ++ tags

      LabeledPoint(label, Vectors.dense(tags))
    }

    nbData.cache()

    val Array(train, test) = nbData.randomSplit(Array(0.999, 0.001))
    val nbModel = NaiveBayes.train(train, lambda = 1.0)

    val nbTotalCorrect = test.map { point =>
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      if (predictLabel == point.label) 1 else 0
    }.sum()
    val nbAccuracy = nbTotalCorrect / test.count()

    println("testNbData cnt : " + test.count())
    println("nbAccuracy: " + nbAccuracy)

    //    println("accuracy : " + accuracy)

    //nbModel.save(sc, args(3))
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }
}
