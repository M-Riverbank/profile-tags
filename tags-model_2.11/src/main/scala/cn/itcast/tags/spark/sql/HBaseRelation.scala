package cn.itcast.tags.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * 自定义外部数据源：从HBase表加载数据和保存数据值HBase表
 *
 * @param context    sparkSQL加载与保存数据入口,相当于SparkSession
 * @param params     参数列表
 * @param userSchema 在SparkSQL中数据封装在DataFrame或者Dataset中Schema信息
 */
case class HBaseRelation(
                          context: SQLContext,
                          params: Map[String, String],
                          userSchema: StructType
                        ) extends BaseRelation
  with TableScan
  with InsertableRelation
  with Serializable {
  // 连接HBase数据库的属性名称
  val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
  val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
  val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
  val HBASE_ZK_PORT_VALUE: String = "zkPort"

  val HBASE_TABLE: String = "hbaseTable"
  val HBASE_TABLE_FAMILY: String = "family"
  val SPERATOR: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
  val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"
  // filterConditions：modified[GE]20190601,modified[LE]20191201
  val HBASE_TABLE_FILTER_CONDITIONS: String = "filterConditions"

  /**
   * sparkSQL加载与保存数据入口,相当于SparkSession
   */
  override def sqlContext: SQLContext = context

  /**
   * 在SparkSQL中数据封装在DataFrame或者Dataset中Schema信息
   */
  override def schema: StructType = userSchema

  /**
   * 从数据源加载数据,封装至RDD[Row]中,结合前面的Schema信息可以生成DataFrame
   */
  override def buildScan(): RDD[Row] = {
    // 1. 设置HBase配置信息
    val conf: Configuration = new Configuration()
    // a. 创建scan对象过滤读取的字段
    val scan: Scan = new Scan()
    // b. 设置读取的列簇
    val familyBytes = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
    scan.addFamily(familyBytes)
    // c. 设置读取的列名称
    val fields: Array[String] = params(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)
    fields.foreach { field =>
      scan.addColumn(familyBytes, Bytes.toBytes(field))
    }
    //===========================此处设置 Hbase Filter 过滤器===========================
    //a.从option参数中获取过滤条件的值，可能没有设置，给以null
    val filterConditions: String = params.getOrElse(HBASE_TABLE_FILTER_CONDITIONS, null)
    //b.判断过滤条件是否有值,如果有值再创建过滤器进行过滤数据
    val filterList: FilterList = new FilterList()
    if (filterConditions != null) {
      //modified[lt]2019-09-01,modified[gt]2019-06-01
      filterConditions
        //多个过滤条件使用逗号分隔
        .split(",")
        //将每个过滤条件构建成 SingleColumnValueFilter 对象
        .foreach { filterCondition =>
          //modified[gt]2019-06-01创建Filter对象
          //step1.解析过滤条件,封装至样例类中
          val condition: Condition = Condition.parseCondition(filterCondition)
          //step2.
          val filter: SingleColumnValueFilter = new SingleColumnValueFilter(
            familyBytes, //列簇
            Bytes.toBytes(condition.field), //字段
            condition.compare, //规则
            Bytes.toBytes(condition.value) //值
          )
          //step3.将filter加入列表
          filterList.addFilter(filter)
          //step4.TODO: 必须获取过滤列的值
          scan.addColumn(familyBytes, Bytes.toBytes(condition.field))
        }
      scan.setFilter(filterList)
    }

    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE)) //zookeeper集群地址
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE)) //zookeeper端口
    conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE)) //读HBase表的名称
    conf.set(
      TableInputFormat.SCAN,
      //scan过滤
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )

    // 2. 调用底层API，读取HBase表的数据
    val datasRDD: RDD[(ImmutableBytesWritable, Result)] =
      sqlContext.sparkContext
        .newAPIHadoopRDD(
          conf,
          classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result]
        )

    // 3. 转换为RDD[Row]
    val rowsRDD: RDD[Row] = datasRDD.map { case (_, result) =>
      // 3.1. 列的值
      val values: Seq[String] = fields.map { field =>
        Bytes.toString(
          result.getValue(familyBytes, Bytes.toBytes(field))
        )
      }
      // 3.2. 生成Row对象
      Row.fromSeq(values)
    }

    // 4. 返回
    rowsRDD
  }

  /**
   * 将DataFrame数据保存至数据源
   *
   * @param data      数据集
   * @param overwrite 覆写
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //1.获取写入的字段列表与列簇
    val fields: Array[String] = data.columns
    val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))

    //2.写入HBase配置项
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE)) //zookeeper地址
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE)) //zookeeper端口号
    conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE)) //写入的表名称

    //3.将DataFrame类型转换为RDD[(ImmutableBytesWritable, Put)]
    val datasRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd
      .map { row =>
        //获取rowKey的值
        val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME)))
        //构建Put对象
        val put: Put = new Put(rowKey)
        //设置列值
        fields.foreach { field =>
          //获取key与value的值并转换为字节数组
          val key: Array[Byte] = Bytes.toBytes(field)
          val value: Array[Byte] = Bytes.toBytes(row.getAs[String](field))
          //写入put对象
          put.addColumn(familyBytes, key, value)
        }
        (new ImmutableBytesWritable(rowKey), put)
      }

    //4.写入HBase表
    datasRDD.saveAsNewAPIHadoopFile(
      s"datas/hbase/output-${System.nanoTime()}",
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )
  }
}
