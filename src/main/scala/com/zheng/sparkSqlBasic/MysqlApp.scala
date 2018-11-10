package com.zheng.sparkSqlBasic

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 外部数据源之
  * 使用Spark操作Mysql数据
  */
object MysqlApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MysqlApp").master("local[2]").getOrCreate()

    /**
      * 通过指定option来加载Mysql数据表
      */
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306")
      .option("dbtable", "class.user")
      .option("user", "root")
      .option("password", "zhengshuangxi")
      .load()

    val hiveDF = spark.read.format("jdbc")
      .option("url", "jdbc:hive2://localhost:10000")
      .option("dbtable","default.person")
      .option("user", "zheng")
      .option("password", "")

    /**
      * 通过传递Properties来加载Mysql数据表
      */
    val connectionProperties = new Properties()
    connectionProperties.put("user","root")
    connectionProperties.put("password", "zhengshuangxi")

    val jdbcDF2 = spark.read.jdbc("jdbc:mysql://localhost:3306", "class.user", connectionProperties)

    /**
      * 通过指定Option来写数据表
      */
    jdbcDF.write.format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306")
        .option("dbtable", "class.user")
        .option("user", "root")
        .option("password", "zhengshuangxi")
        .save()

    /**
      * 通过指定Properties来写数据表
      */
    jdbcDF2.write.jdbc("jdbc:mysql://localhost:3306", "class.user", connectionProperties)
    jdbcDF.show()
    jdbcDF2.show()


    spark.stop()
  }
}
