package com.zheng.sparkSqlBasic


import org.apache.spark.sql.SparkSession

object HiveApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HiveApp")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()


    val hiveDF = spark.table("person")

    hiveDF.show()

    spark.stop()
  }

}
