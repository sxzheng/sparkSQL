package com.zheng.immocLog.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Schema定义及转换
  */
object AccessConvertUtil {

  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("time", StringType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("traffic", LongType),
      StructField("day", StringType)
    )
  )

  /**
    * 解析log,生成Row
    *
    * @param log
    * @return
    */
  def parseLog(log: String) = {
    //2016-11-10 12:01:02     http://www.imooc.com/video/3237 39.186.247.142  317
    val splits = log.split("\t")

    val url = splits(1)
    val domain = "http://www.imooc.com/"
    val cms = url.substring(url.indexOf(domain) + domain.length)
    val cmsTypeId = cms.split("/")
    var cmsType = ""
    var cmsId = 0l
    if (cmsTypeId.length > 1) {
      cmsType = cmsTypeId(0)
      cmsId = cmsTypeId(1).toLong
    }
    val time = splits(0)
    val ip = splits(2)
    val city = IpUtil.getCity(ip)
    val traffic = splits(3).toLong
    val day = time.split(" ")(0).replace("-","")

    Row(url, cmsType, cmsId, time, ip, city, traffic, day)
  }

}
