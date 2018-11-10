package com.zheng.sparkSqlBasic

import org.apache.spark.sql.SparkSession

/**
  * Dataset操作
  */
object DatasetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    val path = "file:///Users/zhengshuangxi/data/people2.csv"

    val df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",";").csv(path)

    import spark.implicits._

    val ds = df.as[People]

    ds.map(line => line.name).show()

    spark.stop()
  }

  case class People(name: String, age: Int, job:String)
}
