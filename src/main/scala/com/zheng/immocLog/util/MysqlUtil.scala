package com.zheng.immocLog.util

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MYSQL数据库连接工具
  */
object MysqlUtil {

  val url = "jdbc:mysql://127.0.0.1:3306/imooc_project"
  val user = "root"
  val password = "zhengshuangxi"
  val driver = "com.mysql.cj.jdbc.Driver"
  var connection : Connection = _
  /**
    * 获取数据库连接
    */
  def getConnection()={
    Class.forName(driver)
    DriverManager.getConnection(url, user, password)
  }

  /**
    * 释放数据库连接资源
    * @param connection
    * @param pstmt
    */
  def release(connection: Connection, pstmt: PreparedStatement)={
    try{
      if(pstmt != null){
        pstmt.close()
      }
    } catch{
      case e : Exception =>{
        e.printStackTrace()
      }
    } finally {
      if(connection != null){
        connection.close()
      }
    }
  }


  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
