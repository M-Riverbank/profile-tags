package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发:用户性别标签模型
 */
class GenderModel
  extends AbstractModel("性别标签", ModelType.MATCH) {
  /*
    314   性别
        315   男   1
        316   女   2
   */
  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    tagTools.ruleMatchTag(businessDF, "gender", tagDF)
  }
}

object GenderModel {
  def main(args: Array[String]): Unit = {
    new GenderModel().executeModel(314)
  }
}
