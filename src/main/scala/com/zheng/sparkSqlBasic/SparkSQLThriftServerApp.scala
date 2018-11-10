package com.zheng.sparkSqlBasic

import java.sql.DriverManager

/**
  * 通过JDBC访问thriftserver
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000","zheng","")

    val pstmt = conn.prepareStatement("select * from person")

    val rs = pstmt.executeQuery()
    while(rs.next()){
      println("name:" + rs.getString("name") +
        " , age:" + rs.getInt("age"))
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}
