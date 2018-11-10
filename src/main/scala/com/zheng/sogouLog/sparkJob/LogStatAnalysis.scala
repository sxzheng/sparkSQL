package com.zheng.sogouLog.sparkJob

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 数据分析
  */
object LogStatAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("LogStatAnalysis").master("local[2]").getOrCreate()

    val logDf = spark.read.format("parquet").load("file:///Users/zhengshuangxi/data/sogouLog/clean")

//    numOfQueries(spark, logDf)

    topNQuery(spark, logDf)

    spark.stop()
  }

  /**
    * 日志总条数
    * @param spark
    * @param logDf
    */
  def numOfQueries(spark: SparkSession, logDf: DataFrame): Unit ={

    println(logDf.count())
  }

  /**
    * 日志非空条数
    * @param spark
    * @param logDf
    */
  def topNQuery(spark: SparkSession, logDf: DataFrame) = {
    import spark.implicits._

    val topNQueryDf = logDf.groupBy("queryWord").agg(count("queryWord").as("times")).orderBy($"times".desc)

    topNQueryDf.show(100)
  }
}
