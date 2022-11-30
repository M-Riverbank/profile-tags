package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.sql.DataFrame


/**
 * 标签模型开发：籍贯标签模型
 */
class NativePlaceModel extends AbstractModel("籍贯标签", ModelType.MATCH) {
  /*
    342   籍贯标签
        343   北京    北京
        344   上海    上海
        345   广州    广州
        346   深圳    深圳
        347   杭州    杭州
        348   苏州    苏州
   */

  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    tagTools.ruleMatchTag(businessDF, "nativePlace", tagDF)
  }
}

object NativePlaceModel {
  def main(args: Array[String]): Unit = {
    new NativePlaceModel().executeModel(342)
  }
}
