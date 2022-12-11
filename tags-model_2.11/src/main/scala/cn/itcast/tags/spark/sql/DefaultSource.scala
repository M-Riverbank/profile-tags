package cn.itcast.tags.spark.sql

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * 默认数据源提供Relation对象，分别为加载数据和保存提供Relation对象
 */
class DefaultSource extends RelationProvider
  with CreatableRelationProvider
  with Serializable
  with DataSourceRegister{

  /**
   * 数据源加载数据使用简短名称
   */
  override def shortName(): String = "hbase"


  val SPERATOR: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"

  /**
   * 从数据源加载读取数据时,创建Relation对象,此Relation实现BaseRelation和TableScan
   *
   * @param sqlContext sparkSQL加载与保存数据入口,相当于SparkSession
   * @param parameters 参数列表
   * @return BaseRelation对象
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]
                             ): BaseRelation = {
    // 1. 定义Schema信息
    val schema: StructType = StructType(
      parameters(HBASE_TABLE_SELECT_FIELDS)
        .split(SPERATOR)
        .map { field =>
          StructField(field, StringType, nullable = true)
        }
    )
    // 2. 创建HBaseRelation对象
    val relation = HBaseRelation(sqlContext, parameters, schema)
    // 3. 返回对象
    relation
  }


  /**
   * 将数据集保存至数据源时,创建Relation对象,此Relation对象实现BaseRelation和InsertableRelation
   *
   * @param sqlContext SparkSession实例对象
   * @param mode       保存模式
   * @param parameters 连接数据源参数
   * @param data       保存的数据
   * @return BaseRelation对象
   */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame
                             ): BaseRelation = {
    //1.创建 HBaseRelation 对象
    val relation: HBaseRelation = HBaseRelation(sqlContext, parameters, data.schema)
    //2.保存数据
    relation.insert(data,overwrite = true)
    //3.返回 Relation 对象
    relation
  }
}
