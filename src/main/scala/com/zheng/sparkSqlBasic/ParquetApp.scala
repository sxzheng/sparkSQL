package com.zheng.sparkSqlBasic

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 外部数据源之
  * 使用Spark操作Parquet数据
  */
object ParquetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("ParquetApp")
      .master("local[2]")
      .getOrCreate()

    /**
      * spark.read.format("parquet").load 这是标准写法
      */
//    val userDF = spark.read.format("parquet").load("file:///Users/zhengshuangxi/data/users.parquet")

    /**
      * spark.read.load 如不指定format，则默认为parquet
      */
//    val userDF2 = spark.read.load("file:///Users/zhengshuangxi/data/users.parquet")

    /**
      * spark.read.format("parquet").option("path","").load() 第三种写法，通过option来指定路径或者其他参数
      */
//    val userDF3 = spark.read.format("parquet").option("path","file:///Users/zhengshuangxi/data/users.parquet").load()

//    userDF.show()
//
//    userDF2.show()
//
//    userDF3.show()

    val userDF4 = spark.sql("SELECT * FROM parquet.`/Users/zhengshuangxi/data/users.parquet`")

    userDF4.show()

//    userDF4.write.partitionBy("name").format("json").save("/Users/zhengshuangxi/data/users.json")
//    spark.sql("create table if not exists users (name string, favorite_color string, favorite_numbers string) USING hive")
    userDF4.write.format("hive").mode(SaveMode.Append).saveAsTable("users")
    spark.stop()
  }
}
