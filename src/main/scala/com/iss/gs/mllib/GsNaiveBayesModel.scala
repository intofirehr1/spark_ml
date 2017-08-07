package com.iss.gs.mllib

import com.iss.gs.util.Utils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, mllib}

/**
  * Created by hadoop on 2017/8/4 0004.
  * 加载模型，进行预测
  */
object GsNaiveBayesModelTest {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("gs_model_test").setMaster(args(0))
    // 设置运行参数： cpu, mem

    // 设置运行参数： cpu, mem
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 加载模型
    val nbModel = NaiveBayesModel.load(spark.sparkContext, args(2))

    val datas = spark.sparkContext.textFile(args(1)).filter(!Utils.isColumnNameLine(_)).map(r => r.split("\t"))

    import spark.implicits._
    val userSchemaString = "MIOS_TRANS_CODE SYS_NO TRANS_CODE TRANS_BAT_NO " +
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

    val schemaFeatures = StructType(userSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    //标签处理，标签的形式：100111  100113  100115  100119  100122  100125  100129, 与数据分隔相同
    val newDatas = datas.map { line =>
      val features = line(line.size - 1).split("_")
      (line.mkString("", "\t", "\t") ++ features.mkString("", "\t", ""))
    }.map(_.split("\t")).map( p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8),
      p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20),
      p(21), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30), p(31), p(32),
      p(33), p(34), p(35), p(36), p(37), p(38), p(39), p(40), p(41), p(42), p(43), p(44),
      p(45), p(46), p(47), p(48), p(49), p(50), p(51), p(52), p(53), p(54), p(55), p(56),
      p(57), p(58), p(59), p(60)))

    val dataDF = spark.createDataFrame(newDatas, schemaFeatures)

    dataDF.cache()

    /** 特征处理*/
    //indexing columns 特征列
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

    val nbModelData = model.transform(dataDF).select("BANKACC_ID_NO", "TRANS_STAT", "AGE_FEATURES_INDEX_VEC",
      "SEX_FEATURES_INDEX_VEC", "ADDRESS_FEATURES_INDEX_VEC", "MARITAL_FEATURES_INDEX_VEC",
      "EDUCATION_FEATURES_INDEX_VEC", "PROFESSION_FEATURES_INDEX_VEC", "VOCATION_FEATURES_INDEX_VEC")
      .map ( x =>
        mllib.regression.LabeledPoint( if(x(1).toString().toDouble == 1) 1 else 0 ,
          mllib.linalg.Vectors.dense(
            x.getAs[SparseVector]("AGE_FEATURES_INDEX_VEC").toArray ++
              x.getAs[SparseVector]("SEX_FEATURES_INDEX_VEC").toArray  ++
              x.getAs[SparseVector]("ADDRESS_FEATURES_INDEX_VEC").toArray ++
              x.getAs[SparseVector]("MARITAL_FEATURES_INDEX_VEC").toArray ++
              x.getAs[SparseVector]("EDUCATION_FEATURES_INDEX_VEC").toArray ++
              x.getAs[SparseVector]("PROFESSION_FEATURES_INDEX_VEC").toArray ++
              x.getAs[SparseVector]("VOCATION_FEATURES_INDEX_VEC").toArray
          )
        )
      )

    // 模型训练数据集缓存
    nbModelData.rdd.cache()

    //val Array(train, test) = nbData.randomSplit(Array(0.999, 0.001))
    //val nbModel = NaiveBayes.train(train, lambda = 1.0)

    //val nbModel = NaiveBayes.train(train, lambda = 1.0)
//    val predictionAndLabel = test.map(p => (nbModel.predict(p.features), p.label))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    val nbTotalCorrect =  nbModelData.rdd.map { point =>
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      if (predictLabel == point.label) 1 else 0
    }.sum()

    val nbAccuracy = nbTotalCorrect /  nbModelData.rdd.count()
    println("testNbData cnt : " +  nbModelData.rdd.count())
    println("accuracy : " + nbAccuracy)

    spark.stop();
  }

}
