package com.iss.gs.test

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, mllib}

/**
  * Created by hadoop on 2017/8/9 0009.
  */

object NaiveBayesGsTagsTest {

  def main(args: Array[String]): Unit = {
    if (args.size < 4) {
      System.err.println("params: master datapath testDataPath modelsavepath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_train").setMaster(args(0))
    // 设置运行参数： cpu, mem
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val datas = spark.sparkContext.textFile(args(1)).filter(!isColumnNameLine(_)).map(r => r.split("\t"))

    import spark.implicits._

    val schemaString = "MIOS_TRANS_CODE SYS_NO TRANS_CODE TRANS_BAT_NO " +
      "TRANS_STAT TRANS_BAT_SEQ PLNMIO_REC_ID BRANCH_BANK_ACC_NO BANK_CODE " +
      "BANK_SUB_CODE BANK_SUB_NAME BANK_PROV_CODE BANK_CITY_CODE BANK_ACC_TYPE " +
      "BANK_ACC_NO ACC_CUST_NAME BANKACC_ID_TYPE BANKACC_ID_NO MGR_BRANCH_NO " +
      "CNTR_NO IPSN_NO TRANS_CLASS MIO_CLASS MIO_ITEM_CODE PLNMIO_DATE MIO_DATE " +
      "TRANS_AMNT CUST_NO GENERATE_DATE UNITE_TRANS_CODE EXT01 EXT02 EXT03 EXT04 " +
      "EXT05 BANK_TRANS_STAT BANK_TRANS_DESC MIO_CUST_NAME GCLK_BRANCH_NO " +
      "GCLK_CLERK_CODE MIO_TX_NO USER_AGE USER_SEX USER_ADDRESS MARITAL " +
      "EDUCATION PROFESSION VOCATION INSURANCE_TYPE ACCORD CUSTOM_LEVEL " +
      "INSURANCE_LEVEL INSURANCE_CHANNEL FEATURES AGE_FEATURES SEX_FEATURES " +
      "ADDRESS_FEATURES MARITAL_FEATURES EDUCATION_FEATURES PROFESSION_FEATURES VOCATION_FEATURES"

    //标签处理，标签的形式：100111  100113  100115  100119  100122  100125  100129, 与数据分隔相同
    val newDatas = datas.map { line =>
      val features = line(line.size - 1).split("_")
      (line.mkString("", "\t", "\t") ++ features.mkString("", "\t", ""))
    }.map(_.split("\t")).map( p => Row(p.toSeq))


    val schemaFeatures = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val dataDF = spark.createDataFrame(newDatas, schemaFeatures)

    // 缓存数据集
    dataDF.cache()

    /** 特征处理*/
    //indexing columns 特征列  Array[String]
    val stringColumns = Array("AGE_FEATURES", "SEX_FEATURES", "ADDRESS_FEATURES", "MARITAL_FEATURES",
      "EDUCATION_FEATURES", "PROFESSION_FEATURES", "VOCATION_FEATURES")
    val index_transformers: Array[org.apache.spark.ml.PipelineStage] = stringColumns.map(
      cname => new StringIndexer()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_INDEX")
    )

    // Add the rest of your pipeline like VectorAssembler and algorithm
    val index_pipeline = new Pipeline().setStages(index_transformers)
    val index_model = index_pipeline.fit(dataDF)
    val df_indexed = index_model.transform(dataDF)

    //encoding columns
    val indexColumns  = df_indexed.columns.filter(x => x contains "INDEX")
    val one_hot_encoders: Array[org.apache.spark.ml.PipelineStage] = indexColumns.map(
      cname => new OneHotEncoder()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_VEC")
    )

    val pipeline = new Pipeline().setStages(index_transformers ++ one_hot_encoders)
    val model = pipeline.fit(dataDF)

    val featuresCol : Array[String] = Array[String]("TRANS_STAT", "AGE_FEATURES_INDEX_VEC",
      "SEX_FEATURES_INDEX_VEC", "ADDRESS_FEATURES_INDEX_VEC", "MARITAL_FEATURES_INDEX_VEC",
      "EDUCATION_FEATURES_INDEX_VEC", "PROFESSION_FEATURES_INDEX_VEC", "VOCATION_FEATURES_INDEX_VEC")

    // TODO 此处需要的是类别，特征向量字段 字段A_INDEX_VEC  格式 类别：特征字段A,特征字段B
    val nbModelData = model.transform(dataDF).select(featuresCol.mkString("",",",""))
        .map ( x =>
        mllib.regression.LabeledPoint( if(x.apply(1).toString().toDouble == 1) 1 else 0 ,
          mllib.linalg.Vectors.dense(
            getEncodeData(x,featuresCol)
//            x.getAs[SparseVector]("AGE_FEATURES_INDEX_VEC").toArray ++
//              x.getAs[SparseVector]("SEX_FEATURES_INDEX_VEC").toArray  ++
//              x.getAs[SparseVector]("ADDRESS_FEATURES_INDEX_VEC").toArray ++
//              x.getAs[SparseVector]("MARITAL_FEATURES_INDEX_VEC").toArray ++
//              x.getAs[SparseVector]("EDUCATION_FEATURES_INDEX_VEC").toArray ++
//              x.getAs[SparseVector]("PROFESSION_FEATURES_INDEX_VEC").toArray ++
//              x.getAs[SparseVector]("VOCATION_FEATURES_INDEX_VEC").toArray
          )
        )
      )

    // 模型训练数据集缓存
    nbModelData.rdd.cache()
//    println(nbModelData.rdd.first())
//    println("======================")

    val Array(train, test) = nbModelData.rdd.randomSplit(Array(0.999, 0.001))
    val nbModel = NaiveBayes.train(train, lambda = 1.0, modelType = "multinomial")

    val nbTotalCorrect = test.map { point =>
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      if (predictLabel == point.label) 1 else 0
    }.sum()

    val nbAccuracy = nbTotalCorrect / test.count()

    println("testNbData cnt : " + test.count())
    println("nbAccuracy: " + nbAccuracy)

    nbModel.save(spark.sparkContext, args(4))
  }

  private def getEncodeData(x : Row, featureCol : Array[String]): Array[Double] = {
    var tmpArray:Array[Double] = Array[Double]()
    for (elem <- featureCol) {
      tmpArray ++ x.getAs[SparseVector](elem++"_INDEX_VEC").toArray
    }
    tmpArray
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

class NaiveBayesGsTagsTest {
}
