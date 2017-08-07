package com.iss.gs.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2017/8/7 0007.
  */

case class UserProp(id:Int, name:String, user_age:Int, user_sex:Int, card_type: String, card_no:String,
                   user_address:String, marital:Int, education: String, insurance_type:String,
                   accord:Double, custom_level:Int, insurance_level:Int, insurance_channel:Int)



object GroupByGS {
  def main(args: Array[String]): Unit = {

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

    // 数据特征字段和特征大类标签需要做对应关系，避免写死特征code TODO

    // 标签数据
    val baseTags = sc.textFile(args(2)).filter(!isColumnNameLine(_)).map(_.split(","))
    val tagsArray = baseTags.collect()
    baseTags.cache()
    def parseAge(array: Array[String]) : String = {
      var returnCode = ""
      for(tags <- tagsArray){
        // 年龄
        val tagValue = tags(2).trim   // 用户信息中的特征数据
        val tagType = tags(4).trim.toInt  // 特征信息中的特征大类
        val tagCode = tags(1).trim    // 特征信息中的特征编码
        if(tagType == 100101){
          val age = array(2).trim.toInt
          //println(tags.foreach(print))
          val values = tagValue.split("-")
          if (values.length == 2)
            if(values(0).trim.toInt <= age && values(1).trim.toInt > age)
              returnCode = parseResult(returnCode, tagCode)
        }

        // 性别
        if(tagType == 100102){
          val sex = array(3).trim.toInt
          if(tagValue.toInt == sex)
            returnCode = parseResult(returnCode, tagCode)
        }

        //地区
        if(tagType == 100103){
          val address = array(6)
          if(tagValue.equals(address))
            returnCode = parseResult(returnCode, tagCode)
        }

        // 婚姻状况
        if(tagType == 100104){
          val marital = array(7).toInt
          if(tagValue.toInt == marital)
            returnCode = parseResult(returnCode, tagCode)
        }

        // 教育程度
        if(tagType == 100105){
          val education = array(8).trim
          if(tagValue.equals(education))
            returnCode = parseResult(returnCode, tagCode)
        }

        // 职业
        if(tagType == 100106){
          val profession = array(9).trim
          if(tagValue.equals(profession))
            returnCode = parseResult(returnCode, tagCode)
        }

        // 行业
        if(tagType == 100107){
          val vocation = array(10).trim
          if(tagValue.equals(vocation))
            returnCode = parseResult(returnCode, tagCode)
        }

      }
      returnCode
    }



    // 根据数据做数据清洗 基本信息标签， 不涉及统计汇总之类操纵
    val userRDD = datas.map(p =>
      // 根据数据来判断标签, 标签中范围标签的设计，
//      (p(0).toInt, p(1), parseAge(p(2).toInt), p(3).toInt, p(4), p(5), addressRule(p(6)),
//        p(7).toInt, educationRule(p(8)), insuranceType(p(9)),p(10).toDouble, p(11).toInt, p(12).toInt, p(13).toInt)
      (p(0), p(1), p(5), parseAge(p))
    )


    // 业务信息标签库，需要设计到业务处理计算
    // TODO

    // userRDD.saveAsTextFile("file://data/gs_train_base2.txt")
    userRDD.collect().foreach(println)

  }

  private def parseResult(code : String, appendval : String): String = {
    var returnStr = ""
    if(!"".equals(code)) {
      returnStr = code + "_" + appendval
    } else {
      returnStr = appendval
    }
    returnStr
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("TAG_CODE")) true
    else false
  }



  /**
    * 年龄段规则
    * @param age
    * @return
    */
  private def ageGroupRule(age:Int): Int = {
    var returnAge: Int = 0
    if(age !=null && age >0){
      if(age <= 20){
        returnAge = 1
      } else if(age <= 30 && age >20){
        returnAge = 2
      } else if(age <= 40 && age >30){
        returnAge = 3
      } else if(age > 40){
        returnAge = 4
      }
    }
    returnAge
  }

  /**
    * 性别规则处理
    * @param sex
    * @return
    */
  private def sexRule(sex : String) : Int = {
    var returnSex:Int = 0
    if(sex != null && sex.toInt == 1){
      returnSex = 1
    }
    returnSex
  }

  /**
    * 保险类型规则
    */
  private def insuranceType( insuranceType : String ) : Int = {
    var insuranceType = 0
    if((insuranceType != null) && !insuranceType.equals(""))
      if(insuranceType.equals("健康险")) insuranceType = 1
      else if(insuranceType.equals("车险")) insuranceType = 2
      else if(insuranceType.equals("旅游险")) insuranceType = 3
      else if(insuranceType.equals("意外险")) insuranceType = 4
      else if(insuranceType.equals("家财险")) insuranceType = 5
    insuranceType
  }

  /**
    * 教育程度规则
    * @param education
    * @return
    */
  private def educationRule(education : String) : Int = {
    var returnEducation = 0
    if(education != null && !education.equals(""))
      if(education.equals("博士")) returnEducation = 1
      else if(education.equals("硕士"))returnEducation = 2
      else if(education.equals("本科"))returnEducation = 3
      else if(education.equals("专科"))returnEducation = 4
      else if(education.equals("高中"))returnEducation = 5

    returnEducation
  }


  private def addressRule(address : String) : Int = {
    var returnAddress = 0
    if(address != null && !address.equals(""))
      if(address.equals("Bei jing")) returnAddress = 1
      else if(address.equals("Shang hai"))returnAddress = 2
      else if(address.equals("Hang zhou"))returnAddress = 3
      else if(address.equals("Shen zhen"))returnAddress = 4
    returnAddress
  }
}
