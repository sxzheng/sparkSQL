package com.zheng.sogouLog.sparkJob

import com.zheng.sogouLog.util.LogConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 数据清洗
  */
object LogStatClean {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("LogStatClean").master("local[2]").getOrCreate()

    // 导入数据
    val logRdd = spark.sparkContext.textFile("file:///Users/zhengshuangxi/data/sogouLog/sogou_500w.log")

//    logRdd.take(10).foreach(println)
    val logDf = spark.createDataFrame(logRdd.map(x => LogConvertUtil.parseLog(x)), LogConvertUtil.struct)

    logDf.show(10)
    logDf.coalesce(1)
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save("/Users/zhengshuangxi/data/sogouLog/clean")

    spark.stop()
  }
}
