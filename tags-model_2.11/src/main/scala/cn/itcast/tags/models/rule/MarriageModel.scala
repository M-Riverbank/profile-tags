package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：婚姻状况标签模型
 */
class MarriageModel extends AbstractModel("婚姻状况标签", ModelType.MATCH) {

  /*
    349   婚姻状况
        350   未婚    1
        351   已婚    2
        352   离异    3
   */


  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    tagTools.ruleMatchTag(businessDF, "marriage", tagDF)
  }
}

object MarriageModel {
  def main(args: Array[String]): Unit = {
    new MarriageModel().executeModel(349)
  }
}

