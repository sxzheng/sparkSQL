package com.zheng.sparkSqlBasic

import org.apache.spark.sql.SparkSession

/**
  * DataFrame的一些操作
  */
object DataFrameApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    val df = spark.read.json("file:///Users/zhengshuangxi/data/people.json")

    // 打印schema
    df.printSchema()

    // 打印前20条数据(默认)
    df.show()

    // select name from table
    df.select("name").show()

    // select name, age+10 from table
    df.select(df.col("name"), (df.col("age") + 10).as("newAge") ).show()

    // select * from table where age > 19
    df.filter(df.col("age") > 19).show()

    // select age, count(1) from table group by age
    df.groupBy(df.col("age")).count().show()

    spark.stop()
  }
}
