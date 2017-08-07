package com.iss.gs.mllib

import java.util

import com.iss.gs.util.{Label2User, MySaveMode, Utils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.{SparkConf, mllib}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by hadoop on 2017/8/7 0007.
  * 模型预测
  */
object GSNavieBayesModelPrognosis {

//  private var broadcastList:Broadcast[util.ArrayList[Row]]  = _

  // 临时存储预测之后的数据
  val label2UserList = new util.ArrayList[Row]()

  def main(args: Array[String]): Unit = {
    if (args.size < 6) {
      System.err.println("params: master datapath schemadata features_msg  modelpath resultsavepath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gs_model_test").setMaster(args(0))
    // 设置运行参数： cpu, mem

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val datas = spark.sparkContext.textFile(args(1)).filter(!Utils.isColumnNameLine(_)).map(r => r.split("\t"))

    import spark.implicits._
    /* 数据列中最后一个为特征列，特征列是所有特征的集合以 _ 分隔，需要拆分最后一列，给根据长度给所有特征赋列名，才可以创建df*/
    val schemaString = spark.sparkContext.textFile(args(2)).collect().mkString("","","")

    /**
      * 读取特征信息，提取出特征个数
      *  问题：如果特征中有特殊情况的未列到特征处理中的怎么办？
      * 先定义，必须要有相关字段，如果有特殊处理，参考年龄的处理方法
      * */
    val featuresMsg = spark.sparkContext.textFile(args(3)).filter(!Utils.isColumnNameLine1(_)).collect()

    //标签处理，标签的形式：100111  100113  100115  100119  100122  100125  100129, 与数据分隔相同
    val newDatas = datas.map { line =>
      val features = line(line.size - 1).split("_")
      (line.mkString("", "\t", "\t") ++ features.mkString("", "\t", ""))
    }.map(_.split("\t")).map( p => Row.fromSeq(p.toSeq))

    var featuresColss : String = ""
    for (elem <- 1 to featuresMsg.size) {
      featuresColss = featuresColss + "FEATURE" + elem + ","
    }

    val schemaFeatures = StructType((schemaString + ",FEATURES," +
      (featuresColss.substring(0,featuresColss.size-1))).split(",").map(fieldName =>
      StructField(fieldName, StringType, true)))

    val dataDF = spark.createDataFrame(newDatas, schemaFeatures)

    // 缓存数据集
    dataDF.cache()

    /**  ================= 特征处理  start ================*/
    //indexing columns 特征列  Array[String]
    val stringColumns = featuresColss.substring(0, featuresColss.size-1).split(",")

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

    val featuresCol : Array[String] =(Array[String]("TRANS_CODE", "TRANS_BAT_NO")) ++  (Array[String]("TRANS_STAT")) ++ (featuresColss.
      substring(0, featuresColss.size-1).split(",").map(x => x + "_INDEX_VEC").array)

    val nbModelData = model.transform(dataDF).select(featuresCol.head, featuresCol.tail: _*)
      .map ( x =>
        (mllib.regression.LabeledPoint(
          if(null != x(2).toString() && !"".equals(x(2).toString()) ) x(2).toString().toDouble else 0,
          //if(x(0).toString().toDouble == 1) 1 else 0 ,
          mllib.linalg.Vectors.dense(
            getEncodeData(x, (featuresColss.substring(0,featuresColss.size-1)).split(","))
          )
        ), x(0).toString, x(1).toString)
      )

    /**  ================= 特征处理  end ================*/

    // 模型训练数据集缓存
    nbModelData.rdd.cache()

    // 加载模型
    val nbModel = NaiveBayesModel.load(spark.sparkContext, args(4))

    /*val nbTotalCorrect =  nbModelData.rdd.map { x =>
      val point = x._1
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      // 保存预测结果和用户信息对应关系
      //saveData(Label2User(x._2, x._3, predictLabel))
      // 正式预测程序中不需要此计算，因为数据中没有point.label 数据或数据为null TODO
      if (predictLabel == point.label) 1 else 0
    } .sum()*/


    val nbTotalArray = nbModelData.rdd.map { x =>
      val point = x._1
      val predictLabel = nbModel.predict(point.features)
      println("predictLabel : " + predictLabel + ", point label : " + point.label + ", point : " + point)
      (point.label, x._2, x._3, predictLabel)
    }.collect()

    for (elem <- nbTotalArray) {
      saveData(Label2User(elem._2, elem._3, elem._4))
    }

   /* println(label2UserList.size())
    println("----------------------------------------------------------")

    broadcastList = spark.sparkContext.broadcast(label2UserList)
    // 保存结果数据

    println(broadcastList.value.size)
    println("=========================================================")*/

    Utils.saveResult(spark, dataDF, label2UserList, args(5), "csv")

    /*val nbAccuracy = nbTotalCorrect /  nbModelData.rdd.count()
    println("testNbData cnt : " +  nbModelData.rdd.count())
    println("accuracy : " + nbAccuracy)*/

//    broadcastList.unpersist()
    nbModelData.rdd.unpersist()

    spark.stop()
  }

  /**
    *
    * @param label2User
    */
  private def saveData(label2User: Label2User): Unit ={
    label2UserList.add(Row(label2User.trans_code, label2User.trans_bat_no, label2User.label.toString))
  }

  private def getEncodeData(x : Row, featureCol : Array[String]): Array[Double] = {
    var tmpArray:Array[Double] = Array[Double]()
    for (elem <- featureCol) {
      tmpArray = tmpArray ++ x.getAs[SparseVector](elem + "_INDEX_VEC").toArray
    }
    tmpArray
  }
}

class GSNavieBayesModelPrognosis{

}