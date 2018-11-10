package com.zheng.immocLog.sparkJob

import com.zheng.immocLog.util.DateUtil
import org.apache.spark.sql.SparkSession

/**
  * 日志第一步清洗
  * 去除移动端日志，非视频/手记日志，非法日志
  * 保存为文本文件
  */
object LogStatFormat {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SparkStatFormat").master("local[2]").getOrCreate()

    val accessRdd = spark.sparkContext.textFile("file:///Users/zhengshuangxi/data/imoocLog/access.log")

    accessRdd.map(
      line => {
        val splits = line.split(" ")

        var url = splits(11).replace("\"", "")
        val domain = "http://www.imooc.com/"
        val time = splits(3) + " " + splits(4)
        val ip = splits(0)
        val traffic = splits(9)

        if((url.contains(domain + "article")
          || url.contains(domain + "video"))
          && !url.substring(url.lastIndexOf("/"), url.length).equals("/publish")){
          if(url.substring(url.lastIndexOf("/"), url.length).equals("/0")){
            url = url.substring(0,url.lastIndexOf("/"))
          }
          if(url.contains("?")){
            url = url.substring(0, url.indexOf("?"))
          }
          DateUtil.parse(time) + '\t' + url + '\t' + ip + '\t' + traffic
        }else{
          ""
        }
      }
    ).filter(_.nonEmpty)
      .saveAsTextFile("file:///Users/zhengshuangxi/data/imoocLog/clean1/")

    spark.stop()
  }
}
