package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{regexp_replace, udf}
import org.apache.spark.sql.types.IntegerType

/**
 * 标签模型开发：年龄段标签模型
 */
class AgeRangeModel extends AbstractModel("年龄段标签", ModelType.STATISTICS) {
  /*
  361   年龄段
      362   50后   19500101-19591231
      363   60后   19600101-19691231
      364   70后   19700101-19791231
      365   80后   19800101-19891231
      366   90后   19900101-19991231
      367   00后   20000101-20091231
      368   10后   20100101-20191231
      369   20后   20200101-20291231
   */

  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   *                   root
   *                   |-- id: long (nullable = false)
   *                   |-- name: string (nullable = true)
   *                   |-- rule: string (nullable = true)
   *                   |-- level: integer (nullable = true)
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._

    //1.针对属性标签数据中的规则rule使用UDF函数，提取start与end
    val TagRuleDF: DataFrame = tagTools.convertTuple(tagDF)

    //2. 业务数据与标签规则关联JOIN，比较范围
    /*
        attrTagDF： attr
        businessDF: business
        SELECT t2.userId, t1.name FROM attr t1 JOIN business t2
        WHERE t1.start <= t2.birthday AND t1.end >= t2.birthday ;
    */
    // 3.1. 转换日期格式： 1982-01-11 -> 19820111
    //a.使用正则函数转换日期并且返回
    businessDF.select(
      $"id",
      regexp_replace($"birthday", "-", "")
        .cast(IntegerType).as("bornDate")
    ) //b.关联属性标签规则数据
      .join(TagRuleDF)
      .where($"bornDate".between($"start", $"end"))
      //c.选取字段
      .select(
        $"id".as("userId"),
        $"name".as("ageRange")
      )
  }
}

object AgeRangeModel {
  def main(args: Array[String]): Unit = {
    new AgeRangeModel().executeModel(361)
  }
}
