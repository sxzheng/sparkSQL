package com.zheng.immocLog.util

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 时间格式转换
  */
object DateUtil {

  val INPUT_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss")

  /**
    * 将长整型时间格式化为目标格式
    * @param time
    * @return
    */
  def parse(time:String)={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 根据字符串获取长整型时间
    * @param time
    * @return
    */
  def getTime(time:String)={
    try{
      INPUT_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime()
    } catch {
      case e: Exception => 0l
    }
  }

  def main(args: Array[String]): Unit = {
//    print(parse("[10/Nov/2016:00:01:02 +0800]"))

    val url = "http://www.imooc.com/video/5475"
    val domain = "http://www.imooc.com/"
    val cmsType = url.substring(url.indexOf(domain) + domain.length, url.lastIndexOf("/"))
    val cmsId = url.substring(url.lastIndexOf("/") + 1, url.length)
    println(cmsType)
    println(cmsId)
  }

}
