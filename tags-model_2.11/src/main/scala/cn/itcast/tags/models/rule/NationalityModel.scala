package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：国籍标签模型
 */
class NationalityModel extends AbstractModel("国籍标签", ModelType.MATCH) {
  /*
      328     国籍
          329     中国大陆
          330     中国香港
          331     中国澳门
          332     中国台湾
          333     其他
   */

  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    tagTools.ruleMatchTag(businessDF, "nationality", tagDF)
  }
}

object NationalityModel {
  def main(args: Array[String]): Unit = {
    new NationalityModel().executeModel(328)
  }
}
