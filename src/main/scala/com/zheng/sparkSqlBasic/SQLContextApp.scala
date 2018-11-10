package com.zheng.sparkSqlBasic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


/**
  * SQLContext的使用
  */
object SQLContextApp {

  def main(args: Array[String]): Unit ={
    val path = args(0)

    // 创建相应的context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // 处理 json
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    // 关闭
    sc.stop()
  }
}
