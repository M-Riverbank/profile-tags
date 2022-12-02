package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：消费周期标签模型
 */
class ConsumeCycleModel extends AbstractModel("消费周期标签", ModelType.STATISTICS) {
  /*
    370     消费周期
        371     近7天     0-7
        372     近2周     8-14
        373     近1月     15-30
        374     近2月     31-60
        375     近3月     61-90
        376     近4月     91-120
        377     近5月     121-150
        378     近半年     151-180
        379     超过半年    181-100000

   */

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
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //1.针对属性标签数据中的规则rule使用UDF函数，提取start与end
    val TagRuleDF: DataFrame = tagTools.convertTuple(tagDF)

    // 2. 订单数据按照会员ID：memberid分组，获取最近一次订单完成时间： finish_time
    val dayDF: DataFrame = businessDF
      // 2.1. 分组，获取最新订单时间，并转换格式
      .groupBy($"memberid")
      .agg(
        from_unixtime(max($"finishtime"))
          //取多个订单中最近的一次时间转换为日期格式
          .as("finish_time")
      )// 2.2. 计算用户最新订单距今天数
    .select(
      $"memberid".as("userId"),
      datediff(current_timestamp(), $"finish_time").as("consumer_days")
    )

    // 3. 关联属性标签数据和消费天数数据，加上判断条件，进行打标签
    val modelDF: DataFrame = dayDF
      .join(TagRuleDF)
      .where($"consumer_days".between($"start", $"end"))
      .select($"userId", $"name".as("consumercycle"))

    modelDF.printSchema()
    modelDF.show(1000)

    // 4. 返回标签数据
//    modelDF
    null
  }
}

object ConsumeCycleModel {
  def main(args: Array[String]): Unit = {
    new ConsumeCycleModel().executeModel(370)
  }
}
