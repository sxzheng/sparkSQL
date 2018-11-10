package com.zheng.immocLog.sparkJob

import com.zheng.immocLog.bean.{DayVideoAccessBean, DayVideoCityAccessBean, TrafficVideoAccess}
import com.zheng.immocLog.dao.resultDAO
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object LogStatAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("LogStatAnalysis").master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("file:///Users/zhengshuangxi/data/imoocLog/clean2")

    //    accessDF.show()
    val day = "20161110"
    //    videoAccessTopNStat(spark, accessDF, day)
//    videoCityAccessTopNStat(spark, accessDF, day)
    trafficTopNStat(spark, accessDF, day)
    spark.stop()
  }

  /**
    * 统计最受欢迎的Video
    *
    * @param spark
    * @param accessDF
    * @return
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String) = {

    import spark.implicits._
    // 过滤出所有video的记录，统计条数
    val resultDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId")
      .agg(count("cmsId").as("times"))
      .orderBy($"times".desc)
      .limit(10)
    resultDF.show()

    try {
      resultDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessBean]

        partitionOfRecords.foreach(record => {
          val day = record.getAs[String]("day")
          val cmsId = record.getAs[Long]("cmsId")
          val times = record.getAs[Long]("times")

          list.append(DayVideoAccessBean(day, cmsId, times))
        })
        resultDAO.insertDayVideoAccess(list)
      })

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

  }

  /**
    * 根据地区统计最受欢迎的Video
    * @param spark
    * @param accessDF
    * @param day
    */
  def videoCityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String) = {

    import spark.implicits._
    //过滤出所有video的记录，结合city统计条数
    val cityVideoDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))
      .orderBy($"times".desc)

    val top3DF = cityVideoDF.select(
      cityVideoDF.col("day"),
      cityVideoDF.col("city"),
      cityVideoDF.col("cmsId"),
      cityVideoDF.col("times"),
      row_number().over(Window.partitionBy("city").orderBy($"times".desc)).as("times_rank")
    ).filter("times_rank <= 3")
    //    cityVideoDF.show()
    //    top3DF.show()
    try {
      top3DF.foreachPartition(partition => {
        val list = new ListBuffer[DayVideoCityAccessBean]
        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val city = record.getAs[String]("city")
          val cmsId = record.getAs[Long]("cmsId")
          val times = record.getAs[Long]("times")
          val times_rank = record.getAs[Int]("times_rank")

          list.append(DayVideoCityAccessBean(day, city, cmsId, times, times_rank))
        })

        resultDAO.insertDayVideoCityAccess(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }

  /**
    * 根据流量统计最受欢迎的Video
    * @param spark
    * @param accessDF
    * @param day
    */
  def trafficTopNStat(spark: SparkSession, accessDF: DataFrame, day: String)={

    import spark.implicits._
    val resultDF = accessDF.filter($"day" === day)
      .groupBy("day", "cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)

    resultDF.show()
    // resultDF写入数据库
    try{

      resultDF.foreachPartition(partition =>{
        val list = new ListBuffer[TrafficVideoAccess]

        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val cmsId = record.getAs[Long]("cmsId")
          val traffics = record.getAs[Long]("traffics")

          list.append(TrafficVideoAccess(day, cmsId, traffics))
        })

        resultDAO.insertTrafficVideoAccess(list)
      })

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
