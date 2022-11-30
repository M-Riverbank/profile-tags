package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame


/**
 * 标签模型开发：学历标签模型
 */
class  EduModel extends AbstractModel("学历标签",ModelType.MATCH){
  /*
    353   学历
        354   小学    小学
        355   初中    初中
        356   高中    高中
        357   大专    大专
        358   本科    本科
        359   研究生   研究生
        360   博士    博士
   */

  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    tagTools.ruleMatchTag(businessDF,"edu",tagDF)
  }
}

object EduModel {
  def main(args: Array[String]): Unit = {
    new EduModel().executeModel(353)
  }
}
