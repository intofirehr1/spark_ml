package com.iss.gs.test

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hadoop on 2017/8/3 0003.
  */
object KMeans_GS {

  def main(args: Array[String]): Unit = {
    if(args.size < 3){
      System.err.println("params: master datapath testDataPath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_train").setMaster(args(0))
    // 设置运行参数： cpu, mem
    val sc = new SparkContext(conf)
    val datas = sc.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))

    // 数据清理,
    // val labelCatgories = datas.map(r => r(4)).distinct().collect().zipWithIndex.toMap

    val sysNo = datas.map(r => r(1)).distinct().collect().zipWithIndex.toMap
    val numSysNo = sysNo.size
//    val transClass = datas.map(r => r(21)).distinct().collect().zipWithIndex.toMap
//    val numTransClass = transClass.size

//    val mioClass = datas.map(r => r(22)).distinct().collect().zipWithIndex.toMap
//    val numMioClass = mioClass.size

    val nbData = datas.map { line =>

      //  TODO 特殊数据处理，如：负数字段处理, 字母处理
      val label = line(4).toInt

      // println("label : " + label)

      // 系统编号处理
      val sysNoIdx = sysNo(line(1))
      val sysNoFeatures = Array.ofDim[Double](numSysNo)
      sysNoFeatures(sysNoIdx) = 1.0

      // 转账类型处理
//      val transClassIdx = transClass(line(21))
//      val transClassFeatures = Array.ofDim[Double](numTransClass)
//      transClassFeatures(transClassIdx) = 1.0

      val transClassFeatures = line.slice(21,22).map(r => if(r.equals("F")) 0.0 else 1.0)

      // 付款类型处理
//      val mioClassIdx = mioClass(line(22))
//      val mioClassFeatures = Array.ofDim[Double](numMioClass)
//      mioClassFeatures(mioClassIdx) = 1.0
      val mioClassFeatures = line.slice(22,23).map(r => if(r.toDouble < 0) 0.0 else r.toDouble)

      val transAmntFeatures = line.slice(26,27).map(r => if(!r.equals("") && r.toDouble>0) r.toDouble else 0.0)
      //println("transAmntFeatures: " + transAmntFeatures.array(0) + ":" + transAmntFeatures.array.length )

      val banksTransStatFeatures = line.slice(35,36).map( r =>
        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3008.00
      )

      val wholeFeatures =  transClassFeatures ++ mioClassFeatures  ++
        transAmntFeatures ++ banksTransStatFeatures ++ sysNoFeatures
      // accuracy : 0.3333333333333333
      // sysNoFeatures ++  transClassFeatures ++ mioClassFeatures ++ transAmntFeatures
      // accuracy : 0.6666666666666666 0.5833333333333334 accuracy : 0.75
      // sysNoFeatures ++  transClassFeatures ++ mioClassFeatures

      LabeledPoint(label, Vectors.dense(wholeFeatures))
    }

    nbData.cache()

    val Array(train, test) = nbData.randomSplit(Array(0.6, 0.4))

    val nbModel = NaiveBayes.train(train, lambda = 1.0)

    val predictionAndLabel = test.map(p => (nbModel.predict(p.features), p.label))

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    val nbTotalCorrect = test.map { point =>
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      if (predictLabel == point.label) 1 else 0
    }.sum()

    val nbAccuracy = nbTotalCorrect / test.count()

    println("testNbData cnt : " + test.count())

    println("nbAccuracy: " + nbAccuracy)
    println("accuracy : " + accuracy)

  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }
}
