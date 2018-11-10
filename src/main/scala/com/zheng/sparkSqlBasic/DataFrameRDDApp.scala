package com.zheng.sparkSqlBasic

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * DataFrame与RDD的互操作
  */
object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

//    rddToDataFrameByReflection(spark)
    rddToDataFrameByPrograme(spark)


    spark.stop()
  }

  private def rddToDataFrameByPrograme(spark: SparkSession) = {
    // 获取rdd
    val rdd = spark.sparkContext.textFile("file:///Users/zhengshuangxi/data/people.csv")

    val peopleRDD = rdd.map(_.split(";")).map(line => Row(line(0), line(1).toInt, line(2)))

    val structType = StructType(Array(StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("job", StringType)))

    val peopleDF = spark.createDataFrame(peopleRDD, structType)

    peopleDF.show()

  }

  private def rddToDataFrameByReflection(spark: SparkSession) = {
    // 获取rdd
    val rdd = spark.sparkContext.textFile("file:///Users/zhengshuangxi/data/people.csv")

    // RDD => DataFrame
    import spark.implicits._
    val peopleDF = rdd.map(_.split(";")).map(line => People(line(0), line(1).toInt, line(2))).toDF()

    peopleDF.show()
  }

  case class People(name: String, age: Int, job:String)
}
