package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.tagTools
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

/**
 * 挖掘类型标签模型开发:客户价值模型
 */
class RfmModel extends AbstractModel("客户价值RFM", ModelType.ML) {
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
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    /*
    385   客户价值
        0   高价值
        1   中上价值
        2   中价值
        3   中下价值
        4   低价值
     */
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    /*
       businessDF.printSchema()
        root
         |-- memberid: string (nullable = true)
         |-- ordersn: string (nullable = true)
         |-- orderamount: string (nullable = true)
         |-- finishtime: string (nullable = true)

     businessDF.show()
    +--------+--------------------+-----------+----------+
    |memberid|             ordersn|orderamount|finishtime|
    +--------+--------------------+-----------+----------+
    |     439|gome_792756751164275|    2479.45|1661624522|
    |     273|jd_14090106121770839|    2449.00|1655557117|
    |     480|jd_14090112394810659|    1099.42|1656133258|
    |     435|amazon_7877495617...|    1999.00|1665484594|
    |     915|jd_14092120154435903|    2488.00|1667303145|
    |     113|jd_14092120155620305|    3449.00|1660162324|
    |     308|suning_8107914555...|    1649.00|1657478747|
    |     879|jd_14092120161884409|       7.00|1658586145|
    |     892|jd_14092120162313538|    1299.00|1662639257|
    |      87|jd_14092120162378713|     499.00|1665487001|
    |      47|jd_14092120162381015|    1899.00|1668051477|
    |     721|jd_14092120163025117|     899.00|1668273964|
    |     269|jd_14092120163320019|    3149.00|1663269000|
    |     158|jd_14092120163848821|     299.00|1656715427|
    |     165|amazon_7956848163...|     289.00|1664018133|
    |     439|jd_14092120165148023|      58.00|1668503389|
    |     476|jd_14092120165516725|    1999.00|1667145843|
    |     295|jd_14092120165617127|    1899.00|1658925714|
    |     377|jd_14092120170327337|    1699.00|1666282078|
    |     236|        rrs_15244971|    1558.00|1655437893|
    +--------+--------------------+-----------+----------+


    tagDF.printSchema()
    tagDF.show()
    root
     |-- id: long (nullable = false)
     |-- name: string (nullable = true)
     |-- rule: string (nullable = true)
     |-- level: integer (nullable = true)
    +---+----+--------------------+-----+
    | id|name|                rule|level|
    +---+----+--------------------+-----+
    |385|客户价值|inType=hbase...     |    4|
    |386| 高价值|                   0|    5|
    |387|中上价值|                   1|    5|
    |388| 中价值|                   2|    5|
    |389|中下价值|                   3|    5|
    |390| 低价值|                   4|    5|
    +---+----+--------------------+-----+
 */
    /*
        TODO: 1·计算每个用户RFM值
            按照用户memberid分组·然后进行娶合函数娶合统计
            R:消费周期·finishtime
                日期时间函数: current_timestamp · from_unixtimestamp · datediff
            F:消费交数ordersn
                count
            M:消费金额orderamount
                sum
     */
    val rfmDF: DataFrame = businessDF
      //a.按照用户id分组,对每个用户的订单数据进行操作
      .groupBy($"memberid")
      .agg(
        max($"finishtime").as("max_finishtime"), //最近消费
        count($"ordersn").as("frequency"), //订单数量
        sum(
          $"orderamount"
            //订单金额String类型转换
            .cast(DataTypes.createDecimalType(10, 2))
        ).as("monetary") //订单总额
      ) //计算R值
      .select(
        $"memberid".as("userId"),
        //计算R值:消费周期
        datediff(
          current_timestamp(), from_unixtime($"max_finishtime")
        ).as("recency"),
        $"frequency",
        $"monetary"
      )
    //    rfmDF.printSchema()
    //    rfmDF.show(10, truncate = false)
    /*
    TODO: 2·按照规则给RFM进行打分（RFM_SCORE)
        R: 1-3天=5分·4-6天=4分·7-9天=3分·10-15天=2分﹐大于16天=1分
        F: >200=5分·150-199=4分·100-149=3分·50-99=2分·1-49=1分
        M: >20w=5分·10-19w=4分·5-9w=3分·1-4w=2分，<1w=1分
        使用CASE WHEN .. WHEN... ELSE .... END
     */
    // R 打分条件表达式
    val rWhen: Column =
    when($"recency".between(1, 3), 5.0) //
      .when($"recency".between(4, 6), 4.0) //
      .when($"recency".between(7, 9), 3.0) //
      .when($"recency".between(10, 15), 2.0) //
      .when($"recency".geq(16), 1.0) //
    // F 打分条件表达式
    val fWhen: Column =
      when($"frequency".between(1, 49), 1.0) //
        .when($"frequency".between(50, 99), 2.0) //
        .when($"frequency".between(100, 149), 3.0) //
        .when($"frequency".between(150, 199), 4.0) //
        .when($"frequency".geq(200), 5.0) //
    // M 打分条件表达式
    val mWhen: Column =
      when($"monetary".lt(10000), 1.0) //
        .when($"monetary".between(10000, 49999), 2.0) //
        .when($"monetary".between(50000, 99999), 3.0) //
        .when($"monetary".between(100000, 199999), 4.0) //
        .when($"monetary".geq(200000), 5.0) //
    val rfmScoreDF: DataFrame = rfmDF.select(
      $"userId", //
      rWhen.as("r_score"), //
      fWhen.as("f_score"), //
      mWhen.as("m_score") //
    )
    /*
    rfmScoreDF.printSchema()
    rfmScoreDF.show(10,truncate = false)
    root
       |-- userId: string (nullable = true)
       |-- r_score: double (nullable = true)
       |-- f_score: double (nullable = true)
       |-- m_score: double (nullable = true)

                                                                                      +------+-------+-------+-------+
      |userId|r_score|f_score|m_score|
      +------+-------+-------+-------+
      |1     |1.0    |5.0    |5.0    |
      |102   |1.0    |3.0    |5.0    |
      |107   |1.0    |3.0    |4.0    |
      |110   |1.0    |3.0    |5.0    |
      |111   |1.0    |3.0    |5.0    |
      |120   |1.0    |3.0    |5.0    |
      |130   |1.0    |2.0    |4.0    |
      |135   |1.0    |2.0    |4.0    |
      |137   |1.0    |3.0    |5.0    |
      |139   |1.0    |3.0    |4.0    |
      +------+-------+-------+-------+
      only showing top 10 rows
     */

    /*
    TODO:3. 训练模型
        KMeans算法，其中K=5
     */
    // 3.1组合R\F\M列为特征值features
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("features")
    val featuresDF: DataFrame = assembler.transform(rfmScoreDF)
    //将训练数据缓存
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK)
    // 3.2 使用KMeans聚类算法模型训并获取模型
    val kMeansModel: KMeansModel = trainModel(featuresDF)
    // 使用模型预测
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
    /*
      root
       |-- userId: string (nullable = true)
       |-- r_score: double (nullable = true)
       |-- f_score: double (nullable = true)
       |-- m_score: double (nullable = true)
       |-- features: vector (nullable = true)
       |-- prediction: integer (nullable = true)
    */
    //    predictionDF.printSchema()
    //    predictionDF.show(50, truncate = false)


    //TODO: 4 打标签
    //4.1聚类类簇关联属性标签数据rule，对应聚类类簇与标签tagName
    val indexTagMap: Map[Int, String] =
    tagTools.convertIndexMap(kMeansModel.clusterCenters, tagDF)
    //使用KMeansModel预测值prediction打标签
    // a.将索引标签Map集合厂广播变量广播出去
    val indexTagMapBroadcast = spark.sparkContext.broadcast(indexTagMap)
    // b.自定义UDF函数,传递预测值 prediction ,返回标签名称 tagName
    val field_to_tag: UserDefinedFunction = udf(
      (clusterIndex: Int) => indexTagMapBroadcast.value(clusterIndex)
    )
    //c.打标签
    val modelDF: DataFrame = predictionDF
      .select(
        $"userId", //用户ID
        field_to_tag(col("prediction")).as("rfm")
      )
    //modelDF.printSchema()
    //modelDF.show(100,truncate = false)
    //返回标签数据
    modelDF
  }

  /**
   * 使用KMeans算法训练模型
   *
   * @param dataframe 数据集
   * @return KMeansModel模型
   */
  def trainModel(dataframe: DataFrame): KMeansModel = {
    //使用KMeans聚类算法模型训练
    val kMeansModel: KMeansModel = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setK(5) // 设置列簇个数：5
      .setMaxIter(20) // 设置最大迭代次数
      .fit(dataframe)
    //均方根误差(越小越好)
    //println(s"WSSSE = ${kMeansModel.computeCost(featuresDF)}")
    //WSSSE = 4.614836295542919E-28
    // 返回
    kMeansModel
  }
}

object RfmModel {
  def main(args: Array[String]): Unit = {
    new RfmModel().executeModel(385)
  }
}
