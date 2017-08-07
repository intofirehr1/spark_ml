package com.iss.gs.test

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by hadoop on 2017/8/10 0010.
  * 特征提取
  */
object FeatureExtractionGSTest {

  def main(args: Array[String]): Unit = {
    if(args.size < 2){
      System.err.println("params: master datapath testDataPath")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("gs_feature_extraction").setMaster(args(0))

    // 设置运行参数： cpu, mem
    val spark = SparkSession.builder().config(conf).getOrCreate()


    // 设置运行参数： cpu, mem
    //val sc = new SparkContext(conf)
//    //val datas = sc.textFile(args(1)).filter(!isColumnNameLine(_)).map(parse)
//    //val srcDatas = sc.textFile(args(1))

    val datas = spark.sparkContext.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))

    datas.cache()

    // 对数据进行特征提取
    // TODO 需要分别对每个特征字段进行抽取，然后对应特征大类信息，总结出一套符合使用规则的特征提取规则

    // 年龄
    val ageNo = datas.map(r => r(41)).distinct().collect().zipWithIndex.toMap

    val sexNo = datas.map(r => r(42)).distinct().collect().zipWithIndex.toMap

    val addressNo = datas.map(r => r(43)).distinct().collect().zipWithIndex.toMap

    val maritalNo = datas.map(r => r(44)).distinct().collect().zipWithIndex.toMap

    val educationNo = datas.map(r => r(45)).distinct().collect().zipWithIndex.toMap

    val professionNo = datas.map(r => r(46)).distinct().collect().zipWithIndex.toMap

    val vocationNo = datas.map(r => r(47)).distinct().collect().zipWithIndex.toMap


    // TODO 制作特征库时，需要先看特征库里的数据是否有变动，或者直接重新生成一套特征库，但是数据一定要变动不大，之前统一的编号一定不能随意变化含义
    val featureTable = "ID,TAG_CODE,TAG_VALUE,TAG_NAME,PARENT_TAG_CODE,PARENT_TAG_NAME,TAG_DESC";
    val schemaFeatures = StructType(featureTable.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val ageGroup = spark.sparkContext.textFile(args(2))  // 固定信息,如： 年龄段，等提前划分好的信息

    // 每个年龄的数据, 或者是规则下的年龄段的数据， 还有，年龄大类的编号要是固定的 111000
    var dflist = new util.ArrayList[Row]()

    var i : Int = 1

    val tmp1 = detailFeaturesCode(ageNo, "年龄", i, 111000, dflist)
    i = tmp1._2
    dflist = tmp1._1
    // 年龄段数据大类的编号是： 111800  年龄段 直接指定就可以，不需要从数据中提取
    val agegroup = ageGroup.collect()
    for (elem <- agegroup) {
     val p = elem.split(",")
      tmp1._1.add(Row(i.toString,p(0),p(1),p(2),p(3),p(4),p(5)))
      i += 1
    }

    // 性别大类112000
    val tmp2 = detailFeaturesCode(sexNo, "性别", i, 112000, dflist)
    i = tmp2._2
    dflist = tmp2._1

    // 地区大类 113000
    val tmp3 = detailFeaturesCode(addressNo, "地区", i, 113000, dflist)
    i = tmp3._2
    dflist = tmp3._1
    // 婚姻状况大类 114000
    val tmp4 = detailFeaturesCode(maritalNo, "婚姻状况", i, 114000, dflist)
    i = tmp4._2
    dflist = tmp4._1
    // 教育程度大类 115000
    val tmp5 = detailFeaturesCode(educationNo, "教育程度", i, 115000, dflist)
    i = tmp5._2
    dflist = tmp5._1
    // 职业大类  116000
    val tmp6 = detailFeaturesCode(professionNo, "职业", i, 116000, dflist)
    i = tmp6._2
    dflist = tmp6._1
    // 行业大类 117000
    val tmp7 = detailFeaturesCode(vocationNo, "行业", i, 117000, dflist)
    i = tmp7._2
    dflist = tmp7._1

    // TODO  业务信息相关大类  21000开头


    // TODO  其他信息依次以31... 41... 等开头


    val dataDF = spark.createDataFrame(dflist, schemaFeatures)
    dataDF.show(200)
    dataDF.rdd.saveAsTextFile(args(3))

  }

  /**
    * 形成特征数据
    * @param featuresNo
    * @param featureName
    * @param i
    * @param pCode
    * @param dflist
    * @return
    */
  private  def detailFeaturesCode (featuresNo: scala.collection.immutable.Map[String, Int],
                                   featureName: String, i : Int, pCode: Int,
                                   dflist : util.ArrayList[Row]) = {
    var cnt = i
    for (elem <- featuresNo) {
      dflist.add(Row(cnt.toString, (pCode + elem._2 + 1).toString, elem._1,
        elem._1, pCode.toString, featureName, elem._1))
      cnt += 1
    }
    (dflist, cnt)
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }

}

class FeatureExtractionGSTest {

}
