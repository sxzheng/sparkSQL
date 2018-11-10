package com.zheng.immocLog.dao

import java.sql.{Connection, PreparedStatement}

import com.zheng.immocLog.bean.{DayVideoAccessBean, DayVideoCityAccessBean, TrafficVideoAccess}
import com.zheng.immocLog.util.MysqlUtil

import scala.collection.mutable.ListBuffer

object resultDAO {

  /**
    * 数据库插入需求一的结果
    * @param list
    */
  def insertDayVideoAccess(list : ListBuffer[DayVideoAccessBean]): Unit ={

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try{
      connection = MysqlUtil.getConnection()
      connection.setAutoCommit(false)

      val sql = "insert into day_video_access(day, cms_id, times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (element <- list){
        pstmt.setString(1, element.day)
        pstmt.setLong(2, element.cmsId)
        pstmt.setLong(3,element.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch() //执行批量处理
      connection.commit()
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MysqlUtil.release(connection, pstmt)
    }
  }


  /**
    * 数据库插入需求二的结果
    * @param list
    */
  def insertDayVideoCityAccess(list : ListBuffer[DayVideoCityAccessBean]): Unit ={

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try{
      connection = MysqlUtil.getConnection()
      connection.setAutoCommit(false)

      val sql = "insert into day_video_city_access(day,city,cms_id,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for(ele <- list){
        pstmt.setString(1, ele.day)
        pstmt.setString(2, ele.city)
        pstmt.setLong(3, ele.cmsId)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.times_rank)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MysqlUtil.release(connection, pstmt)
    }
  }

  def insertTrafficVideoAccess(list: ListBuffer[TrafficVideoAccess])={

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try{
      connection = MysqlUtil.getConnection()
      connection.setAutoCommit(false)

      val sql = "insert into traffic_video_access(day, cmsId, traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for(ele <- list){
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtil.release(connection, pstmt)
    }
  }
}
