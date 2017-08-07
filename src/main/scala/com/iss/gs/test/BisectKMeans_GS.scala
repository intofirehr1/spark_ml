package com.iss.gs.test

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2017/8/4 0004.
  */
object BisectKMeans_GS {

  def main(args: Array[String]): Unit = {

    // 对用户数据进行聚类操作，可能对数据进行分散标签。
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
    val insuranceTypeNo = datas.map(r => r(46)).distinct().collect().zipWithIndex.toMap
    val numInsuranceTypeNo = insuranceTypeNo.size

//    val lrData = datas.map { line =>
//      // 系统编号处理
//      val sysNoIdx = sysNo(line(1))
//      val sysNoFeatures = Array.ofDim[Double](numSysNo)
//      sysNoFeatures(sysNoIdx) = 1.0
//
//      // 转账类型处理
//      val transClassFeatures = line.slice(21,22).map(r => if(r.equals("F")) 0.0 else 1.0)
//      // 付款类型处理
//      val mioClassFeatures = line.slice(22,23).map(r => if(r.toDouble < 0) 0.0 else r.toDouble)
//      // 金额
//      val transAmntFeatures = line.slice(26,27).map(r => if(!r.equals("") && r.toDouble>0) r.toDouble else 0.0)
//      // 银行回复状态
//      val banksTransStatFeatures = line.slice(35,36).map( r =>
//        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3008.00
//      )
//
//      // 增加其他特征值 如：年龄，性别，地址，婚姻状况,学历，险种，金额，客户价值，对保险的认知程度，渠道
//      // 地址处理
//      val addressNoIdx = addressNo(line(43))
//      val addressNoFeatures = Array.ofDim[Double](numAddressNo)
//      addressNoFeatures(addressNoIdx) = 1.0
//
//      // 学历处理
//      val educationNoIdx = educationNo(line(45))
//      val educationNoFeatures = Array.ofDim[Double](numEducationNo)
//      educationNoFeatures(educationNoIdx) = 1.0
//
//      // 险种处理
//      val insuranceTypeNoIdx = insuranceTypeNo(line(46))
//      val insuranceTypeNoFeatures = Array.ofDim[Double](numInsuranceTypeNo)
//      insuranceTypeNoFeatures(insuranceTypeNoIdx) = 1.0
//
//      //性别
//      val userSexFeatures = line.slice(42,43).map( r =>
//        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3.0
//      )
//
//      //婚姻状况
//      val userMaritalFeatures = line.slice(44,45).map( r =>
//        if(!r.equals("") && r.toDouble>=0) r.toDouble else 3.0
//      )
//
//      //客户价值，对保险的认知程度，渠道
//      val otherFeatures = line.slice(48,line.size-1).map( r =>
//        if(!r.equals("") && r.toDouble>=0) r.toDouble else 0.0
//      )
//
//      val wholeFeatures = userSexFeatures ++ mioClassFeatures  ++
//        sysNoFeatures ++
//        addressNoFeatures ++ educationNoFeatures ++ insuranceTypeNoFeatures ++
//        transClassFeatures ++ userMaritalFeatures ++ otherFeatures
//      //++ banksTransStatFeatures ++ transAmntFeatures
//
//      Vectors.dense(wholeFeatures)
//  }

    val lrData = datas.map { line =>

    }


    datas.cache()


    //lrData.collect()
    // 标签是需要预定义还是需要在计算过程中自动生成？（自动生成不靠谱，还是要定义）
//        val k = 10
////         Clustering the data into 6 clusters by BisectingKMeans.
//        val bkm = new BisectingKMeans().setK(k)  // 6
//        val model = bkm.run(datas)
//
//        println(s"Compute Cost: ${model.computeCost(lrData)}")
//        model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
//          println(s"Cluster Center ${idx}: ${center}")
//        }
    //}


  }


private def isColumnNameLine(line:String):Boolean = {
  if (line != null &&
  line.contains("MIOS_TRANS_CODE")) true
  else false
}

  def parse(line: String): Vector = Vectors.dense(line.split("\t").map(_.toDouble))
}
