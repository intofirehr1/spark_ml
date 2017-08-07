package com.iss.gs

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2017/8/4 0004.
  */
object NaiveBayesModelDemo {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("gs_train_model").setMaster(args(0))
    // 设置运行参数： cpu, mem

    val sc = new SparkContext(conf)


    // 加载模型
    //val nbModel = NaiveBayesModel.load(sc, args(2))

    val datas = sc.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))

    // 数据清理 TODO

    // 类别处理 系统编号 地址，学历，险种需要处理类别
    val sysNo = datas.map(r => r(1)).distinct().collect().zipWithIndex.toMap
    val numSysNo = sysNo.size

    // 地址
    val addressNo = datas.map(r => r(43)).distinct().collect().zipWithIndex.toMap
    val numAddressNo = addressNo.size

    // 学历
    val educationNo = datas.map(r => r(45)).distinct().collect().zipWithIndex.toMap
    val numEducationNo = educationNo.size

    // 险种
    val insuranceTypeNo = datas.map(r => r(48)).distinct().collect().zipWithIndex.toMap
    val numInsuranceTypeNo = insuranceTypeNo.size

    val nbData = datas.map { line =>
      //  TODO 特殊数据处理，如：负数字段处理, 字母处理
      val label = line(4).toInt
//      val label = if(line(4).toInt ==1 ) 1 else 0

      // 系统编号处理
      val sysNoIdx = sysNo(line(1))
      val sysNoFeatures = Array.ofDim[Double](numSysNo)
      sysNoFeatures(sysNoIdx) = 1.0

      // 转账类型处理
      val transClassFeatures = line.slice(21,22).map(r => if(r.equals("F")) 0.0 else 1.0)
      // 付款类型处理
      val mioClassFeatures = line.slice(22,23).map(r => if(r.toDouble < 0) 0.0 else r.toDouble)
      // 金额
      val transAmntFeatures = line.slice(26,27).map(r => if(!r.equals("") && r.toDouble>0) r.toDouble else 0.0)
      // 银行回复状态
      val banksTransStatFeatures = line.slice(35,36).map( r =>
        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3008.00
      )

      // 增加其他特征值 如：年龄，性别，地址，婚姻状况,学历，险种，金额，客户价值，对保险的认知程度，渠道
      // 地址处理
      val addressNoIdx = addressNo(line(43))
      val addressNoFeatures = Array.ofDim[Double](numAddressNo)
      addressNoFeatures(addressNoIdx) = 1.0

      // 学历处理
      val educationNoIdx = educationNo(line(45))
      val educationNoFeatures = Array.ofDim[Double](numEducationNo)
      educationNoFeatures(educationNoIdx) = 1.0

      // 险种处理
      val insuranceTypeNoIdx = insuranceTypeNo(line(48))
      val insuranceTypeNoFeatures = Array.ofDim[Double](numInsuranceTypeNo)
      insuranceTypeNoFeatures(insuranceTypeNoIdx) = 1.0

      //性别
      val userSexFeatures = line.slice(42,43).map( r =>
        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3.0
      )

      //婚姻状况
      val userMaritalFeatures = line.slice(44,45).map( r =>
        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3.0
      )

      //客户价值，对保险的认知程度，渠道
      val otherFeatures = line.slice(50,line.size-2).map( r =>
        if(!r.equals("") && r.toDouble>=0) r.toDouble else 0.0
      )
      // 标签
      val tags = line(53).split("_").map(_.toDouble)

      val wholeFeatures = userSexFeatures ++ mioClassFeatures  ++
        sysNoFeatures ++
        addressNoFeatures ++ educationNoFeatures ++ insuranceTypeNoFeatures ++
        transClassFeatures ++ userMaritalFeatures ++ otherFeatures ++ tags
      //++ banksTransStatFeatures ++ transAmntFeatures

      LabeledPoint(label, Vectors.dense(wholeFeatures))
    }


    val Array(train, test) = nbData.randomSplit(Array(0.999, 0.001))
    val nbModel = NaiveBayes.train(train, lambda = 1.0)

    //val nbModel = NaiveBayes.train(train, lambda = 1.0)
//    val predictionAndLabel = test.map(p => (nbModel.predict(p.features), p.label))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    val nbTotalCorrect = test.map { point =>
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      if (predictLabel == point.label) 1 else 0
    }.sum()

    val nbAccuracy = nbTotalCorrect / test.count()
    println("testNbData cnt : " + test.count())
    println("accuracy : " + nbAccuracy)

  }


  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }

}
