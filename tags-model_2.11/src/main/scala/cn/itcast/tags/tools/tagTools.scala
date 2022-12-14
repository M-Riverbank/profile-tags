package cn.itcast.tags.tools

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 针对标签进行相关操作工具类
 */
object tagTools extends Logging {

  /**
   * 将[属性标签(5级标签)]数据中[规则：rule与名称：name]转换为[Map集合]
   *
   * @param tagDF 属性标签数据
   * @return Map[String,String] 集合
   */
  private def convertMap(tagDF: DataFrame): Map[String, String] = {
    import tagDF.sparkSession.implicits._
    tagDF
      // 获取属性标签(5级标签)数据
      .filter($"level" === 5)
      // 选择标签规则rule和标签Id
      .select($"rule", $"name")
      // 转换为Dataset
      .as[(String, String)]
      // 转换为RDD
      .rdd
      // 转换为Map集合
      .collectAsMap().toMap
  }

  /**
   * 依据[标签业务字段的值]与[标签规则]匹配，进行打标签（userId, tagName)
   *
   * @param dataframe 标签业务数据
   * @param field     标签业务字段
   * @param tagDF     5级标签数据
   * @return 标签模型数据
   */
  def ruleMatchTag(
                    dataframe: DataFrame,
                    field: String,
                    tagDF: DataFrame
                  ): DataFrame = {
    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._
    // 1. 获取规则rule与tagId集合
    val attrTagRuleMap: Map[String, String] = convertMap(tagDF)
    // 2. 将Map集合数据广播出去
    val attrTagRuleMapBroadcast = spark.sparkContext.broadcast(attrTagRuleMap)
    // 3. 自定义UDF函数, 依据Job职业和属性标签规则进行标签化
    val field_to_tag: UserDefinedFunction = udf(
      (field: String) => attrTagRuleMapBroadcast.value(field)
    )
    // 4. 计算标签，依据业务字段值获取标签ID
    val modelDF: DataFrame = dataframe
      .select(
        $"id".as("userId"),
        field_to_tag(col(field)).as(field)
      )
    // 5. 返回计算标签数据
    modelDF
  }

  /**
   * 将标签数据中属性标签规则rule拆分为范围: start, end
   *
   * @param tagDF 标签数据
   * @return 数据集DataFrame
   */
  def convertTuple(tagDF: DataFrame): DataFrame = {
    // 导入隐式转换和函数库
    import org.apache.spark.sql.functions._
    import tagDF.sparkSession.implicits._
    //1.自定义udf函数,解析属性标签规则rule
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.trim.split("-").map(_.toInt)
        (start, end)
      }
    )

    //2.针对属性标签数据中的规则rule使用UDF函数，提取start与end并返回数据
    tagDF
      .filter($"level" === 5) // 5级标签
      .select(
        $"name",
        rule_to_tuple($"rule").as("rules")
      )
      // 获取起始start和结束end
      .select(
        $"name",
        $"rules._1".as("start"),
        $"rules._2".as("end")
      )
  }
}
