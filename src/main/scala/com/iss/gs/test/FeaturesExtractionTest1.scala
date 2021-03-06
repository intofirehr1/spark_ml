package com.iss.gs.test

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by hadoop on 2017/8/10 0010.
  */
object FeaturesExtractionTest1 {

  def main(args: Array[String]): Unit = {

    if(args.size < 5){
      System.err.println("params: master datapath featureGroup featuresMsg feature_savepath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_feature_extraction").setMaster(args(0))
    // 设置运行参数： cpu, mem
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val datas = spark.sparkContext.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))
    val dataCols = "MIOS_TRANS_CODE,SYS_NO,TRANS_CODE,TRANS_BAT_NO," +
      "TRANS_STAT,TRANS_BAT_SEQ,PLNMIO_REC_ID,BRANCH_BANK_ACC_NO," +
      "BANK_CODE,BANK_SUB_CODE,BANK_SUB_NAME,BANK_PROV_CODE," +
      "BANK_CITY_CODE,BANK_ACC_TYPE,BANK_ACC_NO,ACC_CUST_NAME," +
      "BANKACC_ID_TYPE,BANKACC_ID_NO,MGR_BRANCH_NO,CNTR_NO," +
      "IPSN_NO,TRANS_CLASS,MIO_CLASS,MIO_ITEM_CODE,PLNMIO_DATE," +
      "MIO_DATE,TRANS_AMNT,CUST_NO,GENERATE_DATE,UNITE_TRANS_CODE," +
      "EXT01,EXT02,EXT03,EXT04,EXT05,BANK_TRANS_STAT,BANK_TRANS_DESC," +
      "MIO_CUST_NAME,GCLK_BRANCH_NO,GCLK_CLERK_CODE,MIO_TX_NO," +
      "USER_AGE,USER_SEX,USER_ADDRESS,MARITAL,EDUCATION,PROFESSION," +
      "VOCATION,INSURANCE_TYPE,ACCORD,CUSTOM_LEVEL,INSURANCE_LEVEL," +
      "INSURANCE_CHANNEL"

    val dataSchema = StructType(dataCols.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val newDatas = datas.map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8),
        p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20),
        p(21), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30), p(31), p(32),
        p(33), p(34), p(35), p(36), p(37), p(38), p(39), p(40), p(41), p(42), p(43), p(44),
        p(45), p(46), p(47), p(48), p(49), p(50), p(51), p(52))
    )
    val dataDF = spark.createDataFrame(newDatas, dataSchema)

    dataDF.cache()

    // 对数据进行特征提取
    // TODO 需要分别对每个特征字段进行抽取，然后对应特征大类信息，总结出一套符合使用规则的特征提取规则
    val featuresParsentMsg = spark.sparkContext.textFile(args(3)).filter(!isColumnNameLine1(_))  // 需要提取的特征字段的信息
    val featuresMsg = featuresParsentMsg.collect()
    var dflist = new util.ArrayList[Row]()
    var i : Int = 1
    for (elem <- featuresMsg) {
      val elemval = elem.split(":")
      val featureNo =  dataDF.select(elemval(1).toUpperCase).rdd.distinct().collect().zipWithIndex.toMap
      //val featureNo = datas.map(r => r(elemval(0).toInt)).distinct().collect().zipWithIndex.toMap
      val tmp1 = detailFeaturesCode(featureNo, elemval(3), i, elemval(2).toInt, dflist)
      i = tmp1._2
      dflist = tmp1._1
    }

    // TODO 制作特征库时，需要先看特征库里的数据是否有变动，或者直接重新生成一套特征库，但是数据一定要变动不大，之前统一的编号一定不能随意变化含义
    val ageGroup = spark.sparkContext.textFile(args(2))  // 固定信息,如： 年龄段，等提前划分好的信息
    // 年龄段数据大类的编号是： 111800  年龄段 直接指定就可以，不需要从数据中提取
    val agegroup = ageGroup.collect()
    for (elem <- agegroup) {
      val p = elem.split(",")
      dflist.add(Row(i.toString,p(0),p(1),p(2),p(3),p(4),p(5)))
      i += 1
    }

    // TODO  业务信息相关大类  21000开头

    // TODO  其他信息依次以31... 41... 等开头

    val featureTable = "ID,TAG_CODE,TAG_VALUE,TAG_NAME,PARENT_TAG_CODE,PARENT_TAG_NAME,TAG_DESC";
    val schemaFeatures = StructType(featureTable.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val featureDF = spark.createDataFrame(dflist, schemaFeatures)
    featureDF.show(200)
    //featureDF.rdd.saveAsTextFile(args(4))
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
  private  def detailFeaturesCode (featuresNo: scala.collection.immutable.Map[Row, Int],
                                   featureName: String, i : Int, pCode: Int,
                                   dflist : util.ArrayList[Row]) = {
    var cnt = i
    for (elem <- featuresNo) {
      dflist.add(Row(cnt.toString, (pCode + elem._2 + 1).toString,  elem._1.getString(0),
        elem._1.getString(0), pCode.toString, featureName,  elem._1.getString(0)))
      cnt += 1
    }
    (dflist, cnt)
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("MIOS_TRANS_CODE")) true
    else false
  }

  private def isColumnNameLine1(line:String):Boolean = {
    if (line != null &&
      line.contains("feature_col_index")) true
    else false
  }
}