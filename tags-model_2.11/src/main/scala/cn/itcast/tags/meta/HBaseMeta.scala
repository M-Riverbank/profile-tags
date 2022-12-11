package cn.itcast.tags.meta

import cn.itcast.tags.utils.DateUtils

/*
  HBase 元数据解析存储，具体数据字段格式如下所示：
      inType=hbase
      zkHosts=bigdata-cdh01.itcast.cn
      zkPort=2181
      hbaseTable=tbl_tag_users
      family=detail
      selectFieldNames=id,gender
      whereCondition=modified#day#30
 */
case class HBaseMeta(
                      zkHosts: String,
                      zkPort: String,
                      hbaseTable: String,
                      family: String,
                      selectFieldNames: String,
                      whereCondition: String
                    )


object HBaseMeta {
  /**
   * 将Map集合数据解析到HBaseMeta中
   *
   * @param ruleMap map集合
   * @return
   */
  def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
    // TODO: 实际开发中，应该先判断各个字段是否有值，没有值直接给出提示，终止程序运行，此处省略
    //依据where语句动态生成过滤条件语句filter
    //TODO: whereCondition=modified#day#30 -> filterConditions=modified[ge]20200907
    //a.从标签规则中获取where的值
    val whereCondition: String = ruleMap.getOrElse("whereCondition", null)
    //b.判断是否有值,有值进行动态生成日期范围
    val filterConditions =
      if (whereCondition != null) {
        //step1.按照分割符进行分割，field=modified、unit=day、amount=30
        val Array(field, unit, amount) = whereCondition.split("#")
        //step2.获取当前日期、昨日日期、N天日期
        val nowDate: String = "2019-10-7"
        val yesterdayDate: String = DateUtils.dateCalculate(nowDate, amount = -1)
        //step3.计算N天日期
        val ageDate = unit match {
          case "day" => DateUtils.dateCalculate(nowDate, amount = -(amount.toInt * 1))
          case "month" => DateUtils.dateCalculate(nowDate, amount = -(amount.toInt * 30))
          case "year" => DateUtils.dateCalculate(nowDate, amount = -(amount.toInt * 365))
        }
        s"$field[le]$yesterdayDate,$field[ge]$ageDate"
      }
      else null

    //构建 HBaseMeta 对象
    HBaseMeta(
      ruleMap("zkHosts"),
      ruleMap("zkPort"),
      ruleMap("hbaseTable"),
      ruleMap("family"),
      ruleMap("selectFieldNames"),
      filterConditions
    )
  }
}

