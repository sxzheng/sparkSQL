package com.zheng.sparkSqlBasic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext的使用方法
  */
object HiveContextApp {
  def main(args: Array[String]): Unit ={

    // 创建相应的context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    // 处理 json
    hiveContext.table("emp").show

    // 关闭
    sc.stop()
  }
}
