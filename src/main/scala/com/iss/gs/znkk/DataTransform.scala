package com.iss.gs.znkk
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  * Created by hadoop on 2017/8/10 0010.
  */
object DataTransform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("gs_data_transform").setMaster(args(0))
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // schema 信息如何获取，是需要读取么？如果是读取的话，那么是不是需要设定每一个表的schema 信息，
    // 这块是需要对所有数据进行一个格式转换，如果是表结构的化，比较好操作，但如果是单纯的文件操作，
    // 同样，需要定义表结构
    val schema : StructType = null
    val dataTransform = new DataTransform
    dataTransform.convert2Parquet(spark, args(1), args(2), schema, "text", "\t")

  }

}

class DataTransform {

  def DataTransform(){}

  /**
    * 数据转换
    */
  def convert2Parquet(spark : SparkSession, src_path : String,
                      dist_path : String, schema : StructType,
                      format : String, split : String): Unit = {
    val df = spark.sqlContext.read.format(format)
            .schema(schema).option("delimiter", split)
            .load(src_path)

    deleteIfExists(spark, dist_path)

    df.write.parquet(dist_path)
  }

  /**
    * 判断文件目录是否存在，如果存在就删除
    * @param src_path
    * @param spark
    */
  def deleteIfExists(spark : SparkSession, src_path : String): Unit ={
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path(src_path)

    if(hdfs.exists(path)){
      // 删除目录，包括目录下的所有数据
      hdfs.delete(path, true)
      // 为防止误删，禁止递归删除
      // hdfs.delete(path, false)
    }
  }

}
