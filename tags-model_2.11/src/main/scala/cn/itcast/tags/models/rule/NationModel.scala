package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：民族标签模型
 */
class  NationModel extends AbstractModel("民族标签",ModelType.MATCH){
/*
    334 民族
        335 汉族      0
        336 蒙古族    1
        337 回族      2
        338 藏族      3
        339 维吾尔族   4
        340 苗族      5
        341 满族      6
 */

  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    tagTools.ruleMatchTag(businessDF,"nation",tagDF)
  }
}

object NationModel {
  def main(args: Array[String]): Unit = {
    new NationModel().executeModel(334)
  }
}
