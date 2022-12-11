package cn.itcast.tags.text

import cn.itcast.tags.utils.DateUtils
import cn.itcast.tags.utils.DateUtils.{dateCalculate, getNow}

object time {
  def main(args: Array[String]): Unit = {
    // 当前日期
    val nowDate: String = getNow()
    println(nowDate)
    // 前一天日期
    val weekDate = dateCalculate(nowDate, -1)
    println(weekDate)
    // 一月前日期
    val weekAfterDate = dateCalculate(nowDate, - 30)
    println(weekAfterDate)
  }
}
