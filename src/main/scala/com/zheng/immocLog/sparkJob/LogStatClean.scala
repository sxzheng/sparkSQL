package com.zheng.immocLog.sparkJob

import com.zheng.immocLog.util.AccessConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 日志第二步清洗
  * 解析访问类型，访问课程/手记ID，解析IP为城市
  * 保存为parquet格式
  */
object LogStatClean {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("LogStatClean").master("local[2]").getOrCreate()

    val accessRdd = spark.sparkContext.textFile("file:///Users/zhengshuangxi/data/imoocLog/clean1/")

    val accessDF = spark.createDataFrame(accessRdd.map(x => AccessConvertUtil.parseLog(x) ),
      AccessConvertUtil.struct)

    accessDF.show()

    accessDF.coalesce(1)
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save("file:///Users/zhengshuangxi/data/imoocLog/clean2/")

    spark.stop()
  }
}
