package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

/**
 * 标签模型开发：支付方式标签模型
 */
class PayTypeModel extends AbstractModel("支付方式标签", ModelType.STATISTICS) {
  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   *
   *         tagDF数据Schema
   *
   *         标签id  ->  id: long (nullable = false)
   *
   *         标签名称  ->  name: string (nullable = true)
   *
   *         标签规则  ->  rule: string (nullable = true)
   *
   *         标签级别  ->  level: integer (nullable = true)
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    /*
      业务数据:
           businessDF.printSchema()
           businessDF.show(5)
               |-- memberid: string (nullable = true)
               |-- paymentcode: string (nullable = true)
           +--------+-----------+
           |memberid|paymentcode|
           +--------+-----------+
           |     439|     alipay|
           |     273|     alipay|
           |     480|     alipay|
           |     435|     alipay|
           |     915|        cod|
           +--------+-----------+
           only showing top 5 rows
    */
    /*
      标签数据:
           tagDF.printSchema()
           tagDF.show()
               |-- id: long (nullable = false)
               |-- name: string (nullable = true)
               |-- rule: string (nullable = true)
               |-- level: integer (nullable = true)
           +---+----+--------------------+-----+
           | id|name|                rule|level|
           +---+----+--------------------+-----+
           |380|支付方式|inType=hbase...     |   4|
           |381| 支付宝|              alipay|    5|
           |382|微信支付|               wxpay|    5|
           |383|银联支付|            chinapay|    5|
           |384|货到付款|                 cod|    5|
           +---+----+--------------------+-----+
     */
    //导入隐式转换
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //1.分析每个用户获取每个用户最多支付方式
    val paymentDF: DataFrame = businessDF
      //a.根据用户id与支付方式分组,统计次数
      .groupBy($"memberid", $"paymentcode")
      .count()
      //b.开窗分析
      .withColumn(
        "rnk",
        row_number().over(Window
          .partitionBy($"memberid")
          .orderBy($"count".desc)
        )
      ) //c.过滤rnk为1的数据
      .where($"rnk" === 1)
      .select(
        $"memberid".as("id"),
        $"paymentcode".as("payment")
      )

    // 2. 计算标签，规则匹配
    tagTools.ruleMatchTag(paymentDF, "payment", tagDF)
    //    frame.printSchema()
    //    frame.show()
    //    null
  }
}

object PayTypeModel {
  def main(args: Array[String]): Unit = {
    new PayTypeModel().executeModel(380)
  }
}