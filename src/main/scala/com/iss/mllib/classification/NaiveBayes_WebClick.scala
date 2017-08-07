package com.iss.mllib.classification
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by hadoop on 2017/8/1 0001.
  */
object NaiveBayes_WebClick {

  def main(args: Array[String]): Unit = {
    if(args.length <2){
      System.err.println("args: master datapath !")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("naviebayes webclick")
                  .setMaster(args(0))
    val sc = new SparkContext(conf)
    val dataRdds = sc.textFile(args(1)).map(line => line.split("\t")) // 解析数据
    val recordfirst = dataRdds.first()


    println(recordfirst.foreach(println _ ))

    /**  数据清理， 过滤数据中多余的“ 和 补充数据中的缺少值 */
//    val nbData = dataRdds.map{ r =>
//      val trimmed = r.map(_.replaceAll("\"",""))  // 过滤多余的引号
//      val lable = trimmed(r.size-1).toInt    //最后一列做为标签
//      val features = trimmed.slice(4, r.size-1).map(d => if(d == "?") 0 else d.toDouble) // 从第四列之后的数据进行特征提取
//      LabeledPoint(lable, Vectors.dense(features))  // 创建一个稠密向量
//    }

    /**   贝叶斯需要特征值必须为非负数 */
    val nbData = dataRdds.map{ r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val lable = trimmed(r.size-1).toInt
      val features = trimmed.slice(4, r.size-1).map(d => if(d == "?") 0.0 else d.toDouble).map(d => if(d < 0) 0.0 else d)
      LabeledPoint(lable, Vectors.dense(features))
    }

    val splits = nbData.randomSplit(Array(0.8, 0.2))

    // 数据量
    val dataNum = nbData.count()
    println("==========".concat("data cnt : ").concat(dataNum + ""))

    // 如果要进行迭代计算等设计到复用的情况，对数据进行缓存
    splits(0).cache() //data.persist()

    // 模型训练
    val nbModel = NaiveBayes.train(splits(0), lambda = 1.0)

    val correct = nbModel.predict(splits(1).map(_.features))

    val nbTotalCorrect = nbData.map( point =>
      if(nbModel.predict(point.features) == point.label) 1 else 0
    ).sum()


    val nbAccuracy = nbTotalCorrect / dataNum
    println("nbAccuracy : " + nbAccuracy)  // 模型准确率

  }
}
