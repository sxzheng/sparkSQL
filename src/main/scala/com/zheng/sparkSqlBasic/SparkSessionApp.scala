package com.zheng.sparkSqlBasic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * SparkSession的使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit ={

    val sparkSession = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

//    sparkSession.read.format("json")
    val people = sparkSession.read.json("file:///Users/zhengshuangxi/data/people.json")
    people.show()

    sparkSession.stop()
  }
}
