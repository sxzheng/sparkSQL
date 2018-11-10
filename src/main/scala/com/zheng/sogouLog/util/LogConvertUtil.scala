package com.zheng.sogouLog.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object LogConvertUtil {

  // 定义输出字段
  val struct = StructType(
    //访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
    Array(
      StructField("time", StringType),
      StructField("day", StringType),
      StructField("userId", StringType),
      StructField("queryWord", StringType),
      StructField("urlRank", IntegerType),
      StructField("clickOrder", IntegerType),
      StructField("url", StringType)
    )
  )

  def parseLog(log: String) = {

    try{
      val splits = log.split("\t")

      val time = DateUtils.parse(splits(0))
      val day = time.split(" ")(0)
      val userId = splits(1)
      val queryWord = splits(2)
      val urlRank = splits(3).toInt
      val clickOrder = splits(4).toInt
      val url = splits(5)

      Row(time, day, userId, queryWord, urlRank, clickOrder, url)
    } catch{
      case e: Exception => Row(0)
    }

  }
}
