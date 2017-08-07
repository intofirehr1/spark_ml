package com.iss.gs.test

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hadoop on 2017/8/9 0009.
  */

object NaiveBayesGsTags {

  def enum2Double(s: String, _1: Any) = ???

  def main(args: Array[String]): Unit = {
    if (args.size < 4) {
      System.err.println("params: master datapath testDataPath modelsavepath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_train").setMaster(args(0))
    // 设置运行参数： cpu, mem

    val sc = new SparkContext(conf)

    //val sqlContext = new SQLContext(sc)

    val datas = sc.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))


    //标签处理，标签的形式：100111  100113  100115  100119  100122  100125  100129, 与数据分隔相同
    val newDatas = datas.map { line =>
      val features = line(line.size - 1).split("_")
      (line.mkString("", "\t", "\t") ++ features.mkString("", "\t", ""))
    }.map(r => r.split("\t"))

    // 特征RDD
    // 类别处理 系统编号 地址，学历，险种需要处理类别
    val sysNo = newDatas.map(r => r(1)).distinct().collect().zipWithIndex.toMap
    val numSysNo = sysNo.size

    // 年龄
    val ageNo = newDatas.map(r => r(54)).distinct().collect().zipWithIndex.toMap
    val numAgeNo = ageNo.size

    // 性别
    val sexNo = newDatas.map(r => r(55)).distinct().collect().zipWithIndex.toMap
    val numSexNo = sexNo.size

    // 地址
    val addressNo = newDatas.map(r => r(56)).distinct().collect().zipWithIndex.toMap
    val numAddressNo = addressNo.size

    // 婚姻状态
    val maritalNo = newDatas.map(r => r(57)).distinct().collect().zipWithIndex.toMap
    val numMaritalNo = maritalNo.size

    // 教育程度
    val educationNo = newDatas.map(r => r(58)).distinct().collect().zipWithIndex.toMap
    val numEducationNo = educationNo.size

    // 职业
    val professionNo = newDatas.map(r => r(59)).distinct().collect().zipWithIndex.toMap
    val numProfessionNo = professionNo.size

    // 行业
    val vocationNo = newDatas.map(r => r(60)).distinct().collect().zipWithIndex.toMap
    val numVocationNo = vocationNo.size

    // 险种
    val insuranceTypeNo = newDatas.map(r => r(48)).distinct().collect().zipWithIndex.toMap
    val numInsuranceTypeNo = insuranceTypeNo.size

    val nbData = newDatas.map { line =>
      //  TODO 特殊数据处理，如：负数字段处理, 字母处理
      val label = line(4).toInt
      // 处理特征， 特征数据起始位置为53，总特征集合字符串
      val tags = line(53).toString.split("_").map(_.toDouble)
      // 特征数量
      val size = tags.size

      // 系统编号处理
      val sysNoIdx = sysNo(line(1))
      val sysNoFeatures = Array.ofDim[Double](numSysNo)
      sysNoFeatures(sysNoIdx) = 1.0

      // 年龄
      val ageNoIdx = ageNo(line(54))
      val ageNoFeatures = Array.ofDim[Double](numAgeNo)
      ageNoFeatures(ageNoIdx) = 1.0

      // 性别
      val sexNoIdx = sexNo(line(55))
      val sexNoFeatures = Array.ofDim[Double](numSexNo)
      sexNoFeatures(sexNoIdx) = 1.0

      // 地址
      val addressNoIdx = addressNo(line(56))
      val addressNoFeatures = Array.ofDim[Double](numAddressNo)
      addressNoFeatures(addressNoIdx) = 1.0

      // 婚姻状况
      val maritalNoIdx = maritalNo(line(57))
      val maritalNoFeatures = Array.ofDim[Double](numMaritalNo)
      maritalNoFeatures(maritalNoIdx) = 1.0

       // 教育程度
      val educationNoIdx = educationNo(line(58).toString)
      val educationNoFeatures = Array.ofDim[Double](numEducationNo)
      educationNoFeatures(educationNoIdx) = 1.0

      // 职业
      val professionNoIdx = professionNo(line(59))
      val professionNoFeatures = Array.ofDim[Double](numProfessionNo)
      professionNoFeatures(professionNoIdx) = 1.0

      // 行业
      val vocationNoIdx = vocationNo(line(60))
      val vocationNoFeatures = Array.ofDim[Double](numVocationNo)
      vocationNoFeatures(vocationNoIdx) = 1.0

      // 险种处理
      val insuranceTypeNoIdx = insuranceTypeNo(line(48).toString)
      val insuranceTypeNoFeatures = Array.ofDim[Double](numInsuranceTypeNo)
      insuranceTypeNoFeatures(insuranceTypeNoIdx) = 1.0

//      //性别
//      val userSexFeatures = line.slice(42,43).map( r =>
//        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3.0
//      )
//
//      //婚姻状况
//      val userMaritalFeatures = line.slice(44,45).map( r =>
//        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3.0
//      )

      //客户价值，对保险的认知程度，渠道
      val otherFeatures = line.slice(50,53).map( r =>
        if(!r.equals("") && r.toDouble>=0) r.toDouble else 0.0
      )

      // 标签离散化
      val wholeFeatures = sysNoFeatures ++ ageNoFeatures ++ sexNoFeatures ++
        addressNoFeatures ++ maritalNoFeatures ++ educationNoFeatures ++
        professionNoFeatures ++ vocationNoFeatures ++ insuranceTypeNoFeatures ++
        otherFeatures

      LabeledPoint(label, Vectors.dense(wholeFeatures))
    }

    nbData.cache()

    val Array(train, test) = nbData.randomSplit(Array(0.8, 0.2))
    val nbModel = NaiveBayes.train(train, lambda = 1.0)

    val nbTotalCorrect = test.map { point =>
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      if (predictLabel == point.label) 1 else 0
    }.sum()
    val nbAccuracy = nbTotalCorrect / test.count()

    println("testNbData cnt : " + test.count())
    println("nbAccuracy: " + nbAccuracy)
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }
}

class NaiveBayesGsTags {

}






