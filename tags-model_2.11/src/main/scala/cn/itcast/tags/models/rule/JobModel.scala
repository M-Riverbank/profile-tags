package cn.itcast.tags.models.rule

import cn.itcast.tags.models.ModelType
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame


/**
 * 标签模型应用开发:用户职业标签
 */
class JobModel extends
  AbstractModel("职业标签", ModelType.MATCH) {
  /*
    317   职业
        318   学生      1
        319   公务员     2
        320   军人      3
        321   警察      4
        322   教师      5
        323   白领      6
 */

  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    tagTools.ruleMatchTag(businessDF, "job", tagDF)
  }
}

object JobModel {
  def main(args: Array[String]): Unit = {
    new JobModel().executeModel(317)
  }
}
