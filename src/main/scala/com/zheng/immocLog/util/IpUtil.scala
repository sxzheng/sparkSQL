package com.zheng.immocLog.util

import com.ggstar.util.ip.IpHelper

/**
  * ip解析为城市
  */
object IpUtil {

  /**
    * 根据ip获取城市
    * @param ip
    * @return
    */
  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("14.153.236.58"))
  }

}
