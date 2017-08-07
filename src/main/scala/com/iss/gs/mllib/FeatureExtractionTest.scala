package com.iss.gs.mllib

import java.util

import com.iss.gs.util.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by hadoop on 2017/8/10 0010.
  * 特征提取
  */
object FeatureExtractionGS {

  def main(args: Array[String]): Unit = {
    if(args.size < 5){
      System.err.println("params: master datapath featureGroup featuresMsg feature_savepath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_feature_extraction").setMaster(args(0))

    // 设置运行参数： cpu, mem
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val datas = spark.sparkContext.textFile(args(1)).filter(!Utils.isColumnNameLine(_)).map(r => r.split("\t"))

    datas.cache()
    import spark.implicits._

    // 对数据进行特征提取
    // TODO 需要分别对每个特征字段进行抽取，然后对应特征大类信息，总结出一套符合使用规则的特征提取规则
    val featuresParsentMsg = spark.sparkContext.textFile(args(3)).filter(!Utils.isColumnNameLine1(_))  // 需要提取的特征字段的信息
    val featuresMsg = featuresParsentMsg.collect()
    var dflist = new util.ArrayList[Row]()
    var i : Int = 1
    for (elem <- featuresMsg) {
      val elemval = elem.split(":")
      val featureNo = datas.map(r => r(elemval(0).toInt)).distinct().collect().zipWithIndex.toMap
      val tmp1 = detailFeaturesCode(featureNo, elemval(3), i, elemval(2).toInt, dflist)
      i = tmp1._2
      dflist = tmp1._1
    }

    // TODO 制作特征库时，需要先看特征库里的数据是否有变动，或者直接重新生成一套特征库，但是数据一定要变动不大，之前统一的编号一定不能随意变化含义
    val featureTable = "ID,TAG_CODE,TAG_VALUE,TAG_NAME,PARENT_TAG_CODE,PARENT_TAG_NAME,TAG_DESC";
    val schemaFeatures = StructType(featureTable.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val ageGroup = spark.sparkContext.textFile(args(2))  // 固定信息,如： 年龄段，等提前划分好的信息

    // 年龄段数据大类的编号是： 111800  年龄段 直接指定就可以，不需要从数据中提取
    val agegroup = ageGroup.collect()
    for (elem <- agegroup) {
      val p = elem.split(",")
      dflist.add(Row(i.toString,p(0),p(1),p(2),p(3),p(4),p(5)))
      i += 1
    }

    // TODO  业务信息相关大类  21000开头
    // 时间段的可以在数据预处理的时候


    // TODO  其他信息依次以31... 41... 等开头

    val dataDF = spark.createDataFrame(dflist, schemaFeatures)
    //dataDF.show(200)
    dataDF.rdd.saveAsTextFile(args(4))

    spark.stop()
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
}

class FeatureExtractionGS {

}
