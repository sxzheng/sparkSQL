package com.zheng.sogouLog.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  //输入时间格式 :20111230000005 yyyyMMddHHmmss
  val INPUT_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  //目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 将输入时间格式转换成目标格式
    * @param time
    */
  def parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 通过时间戳获取输入格式的时间
    * @param time
    * @return
    */
  def getTime(time:String)={
    try{
      INPUT_FORMAT.parse(time).getTime
    } catch{
      case e : Exception => 0l
    }
  }

  def main(args: Array[String]) {
    println(parse("20111230000005"))
  }

}
