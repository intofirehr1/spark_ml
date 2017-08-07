package com.iss.gs.mllib

import com.iss.gs.util.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control._
/**
  * Created by hadoop on 2017/8/8 0008.
  * 用户特征
  */
object Features2UserGS {

  def main(args: Array[String]): Unit = {

    if (args.size < 5) {
      System.err.println("params: master data_path feature_data_path feature_extraction_col_path resutl_save_path")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_user2features").setMaster(args(0))
    // 设置运行参数： cpu, mem
   // val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val datas = spark.sparkContext.textFile(args(1)).filter(!Utils.isColumnNameLine(_)).map(r => r.split("\t"))

    // 数据特征字段和特征大类标签需要做对应关系，避免写死特征code TODO

    // 标签数据
    val baseTags = spark.sparkContext.textFile(args(2)).filter(!isColumnNameLine(_)).map(_.split(","))
    val tagsArray = baseTags.collect()
    baseTags.cache()

    val featuresParsentMsg = spark.sparkContext.textFile(args(3)).filter(!Utils.isColumnNameLine1(_))  // 需要提取的特征字段的信息
    val featuresMsg = featuresParsentMsg.collect()

    def parseAge(array: Array[String]): String = {
      var returnCode = ""
      for (tags <- tagsArray) {
        val tagValue = tags(2).trim // 特征信息中的特征数据
        val featureType = tags(4).trim.toInt // 特征信息中的特征大类
        val tagCode = tags(1).trim // 特征信息中的特征编码

        returnCode = parseFeature(returnCode, featuresMsg, featureType, array, tagValue, tagCode)

        // 年龄段  // 111000 年龄
        if (featureType == 111800) {
          val age = array(41).trim.toInt
          //println(tags.foreach(print))
          val values = tagValue.split("-")
          if (values.length == 2)
            if (values(0).trim.toInt <= age && values(1).trim.toInt > age)
              returnCode = parseResult(returnCode, tagCode)
        }
      }
      returnCode
    }

    // 根据数据做数据清洗 基本信息标签， 不涉及统计汇总之类操纵
    val userRDD = datas.map(p =>
      // 根据数据来判断标签, 标签中范围标签的设计，
      (p.mkString("", "\t", "\t") ++ parseAge(p))
    )

    // TODO 业务信息标签库，需要设计到业务处理计算
//    userRDD.collect().take(10).foreach(println)
    userRDD.saveAsTextFile(args(4))

    spark.stop()
  }

  /**
    *  处理用户特征和标签之间的联系，并未用户数据打标签组合
    * @param returnCode
    * @param featuresMsg
    * @param featureType
    * @param array
    * @param tagValue
    * @param tagCode
    * @return
    */
  private def parseFeature(returnCode : String, featuresMsg : Array[String],
                           featureType : Int, array: Array[String],
                           tagValue: String, tagCode : String) : String = {
    // TODO 需要考虑几个问题： 数值和特征不符的情况，是要在之前补充数据还是要删除数据。
    var tmp = returnCode
    val loop = new Breaks;
    loop.breakable {
      for (elem <- featuresMsg) {
        val elemval = elem.split(":")
        // 过滤掉特征大类处理
        if (featureType != 111000 && featureType != 111800)
          if ((featureType == elemval(2).toInt) &&
            (tagValue.equals(array(elemval(0).toInt).trim))) {
            tmp = parseResult(tmp, tagCode)
            loop.break
          }
      }
    }
    tmp
  }


  private def parseResult(code : String, appendval : String): String = {
    //println(" code :" + code + ", appendval : " + appendval)
    if(!"".equals(code)) {
      code ++ "_" ++ appendval
    } else {
      appendval
    }
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("TAG_CODE")) true
    else false
  }

}

class Features2UserGS{

}