package cn.itcast.tags.models

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

abstract class AbstractModel(modelName: String, modelType: ModelType) extends Logging {
  // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
  System.setProperty("user.name", ModelConfig.FS_USER)
  System.setProperty("HADOOP_USER_NAME", ModelConfig.FS_USER)
  // 变量声明
  var spark: SparkSession = _

  /**
   *  1. 初始化：构建SparkSession实例对象
   */
  def init(isHive: Boolean): Unit = {
    spark = SparkUtils.createSparkSession(this.getClass, isHive)
  }

  /**
   * 2. 标签数据：依据标签ID从MySQL数据库表tbl_basic_tag获取标签数据
   *
   * @param tagId 4级属性的标签id，根据这个id获取4级标签以及对应的5级标签的数据
   * @return 4级及其拥有的5级标签的数据
   */
  def getTagData(tagId: Long): DataFrame = {
    //读取标签数据
    spark.read
      .format("jdbc")
      .option("driver", ModelConfig.MYSQL_JDBC_DRIVER)
      .option("url", ModelConfig.MYSQL_JDBC_URL)
      .option("dbtable", ModelConfig.tagTable(tagId))
      .option("user", ModelConfig.MYSQL_JDBC_USERNAME)
      .option("password", ModelConfig.MYSQL_JDBC_PASSWORD)
      .load()
  }

  /**
   * 3. 业务数据：依据业务标签规则rule，从数据源获取业务数据
   *
   * @param tagDF 标签属性数据
   * @return 业务数据
   */
  def getBusinessData(tagDF: DataFrame): DataFrame = {
    import tagDF.sparkSession.implicits._
    //a.获取4级标签规则并封装map集合
    val tagRuleMap: Map[String, String] = tagDF
      .filter($"level" === 4) //业务标签属于四级标签
      .head() //返回Row对象
      .getAs[String]("rule")
      .split("\\n")
      .map { line =>
        val Array(key, value) = line.trim.split("=")
        (key, value)
      }.toMap
    logWarning(s"==================< ${tagRuleMap.mkString(",")} >==================")

    //b.根据标签规则集成Hbase读取需要的业务数据
    var businessDF: DataFrame = null
    if ("hbase".equals(tagRuleMap("inType").toLowerCase)) {
      //封装标签规则中数据源的信息至HBaseMeta对象中
      val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(tagRuleMap)
      //从Hbase表加载数据
      businessDF = spark.read
        .format("hbase")
        .option("zkHosts", hbaseMeta.zkHosts)
        .option("zkPort", hbaseMeta.zkPort)
        .option("hbaseTable", hbaseMeta.hbaseTable)
        .option("family", hbaseMeta.family)
        .option("selectFields", hbaseMeta.selectFieldNames)
        .load()
    } else {
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    //c.返回业务数据
    businessDF
  }

  /**
   * 4. 构建标签：依据业务数据和属性标签数据建立标签
   *
   * @param businessDF 需要进行处理的业务数据
   * @param tagDF      标签数据，其中包含4级与5级标签的name与rule
   * @return 打完标签的数据
   *
   *         tagDF数据Schema
   *
   *         标签id  ->  id: long (nullable = false)
   *
   *         标签名称  ->  name: string (nullable = true)
   *
   *         标签规则  ->  rule: string (nullable = true)
   *
   *         标签级别  ->  level: integer (nullable = true)
   */
  def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

  /**
   * 5. 保存画像标签数据至HBase表
   *
   * @param modelDF 需要进行保存的打完标签的数据
   */
  def saveTag(modelDF: DataFrame): Unit = {
    if (modelDF != null)
      modelDF.write
        .mode(SaveMode.Overwrite)
        .format("hbase")
        .option("zkHosts", ModelConfig.PROFILE_TABLE_ZK_HOSTS)
        .option("zkPort", ModelConfig.PROFILE_TABLE_ZK_PORT)
        .option("hbaseTable", ModelConfig.PROFILE_TABLE_NAME)
        .option("family", ModelConfig.PROFILE_TABLE_FAMILY_USER)
        .option("rowKeyColumn", ModelConfig.PROFILE_TABLE_ROWKEY_COL)
        .save()
  }

  /**
   * 6. 关闭资源：应用结束，关闭会话实例对象
   */
  def close(): Unit = {
    if (spark != null) spark.stop()
  }

  /**
   * 规定标签模型执行流程顺序
   *
   * @param tagId 该计算模型的4级标签id
   */
  def executeModel(tagId: Long, isHive: Boolean = false): Unit = {
    // a. 初始化
    init(isHive)
    try {
      // b. 获取标签数据
      val tagDF: DataFrame = getTagData(tagId)
      tagDF.persist(StorageLevel.MEMORY_AND_DISK)
      tagDF.count()
      // c. 获取业务数据
      val businessDF: DataFrame = getBusinessData(tagDF)
      // d. 计算标签
      val modelDF: DataFrame = doTag(businessDF, tagDF)
      // e. 保存标签
      saveTag(modelDF)
      tagDF.unpersist()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //f.关闭资源
      close()
    }
  }
}
