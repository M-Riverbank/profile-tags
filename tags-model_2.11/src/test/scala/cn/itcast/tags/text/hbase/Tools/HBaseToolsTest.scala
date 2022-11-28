package cn.itcast.tags.text.hbase.Tools

import cn.itcast.tags.tools.HBaseTools
import org.apache.spark.sql.{DataFrame, SparkSession}

object HBaseToolsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    /*
        zkHosts=bigdata-cdh01.itcast.cn
        zkPort=2181
        hbaseTable=tbl_tag_users
        family=detail
        selectFieldNames=id,gender
    */
    val df: DataFrame = HBaseTools.read(
      spark,
      "bigdata-cdh01.itcast.cn",
      "2181",
      "tbl_tag_users",
      "detail",
      Seq("id", "gender")
    )

    println(s"count = ${df.count}")
    df.printSchema()

    // TODO: 5. 将标签数据存储到HBase表中：用户画像标签表 -> tbl_profile
    HBaseTools.write(
      df,
      "bigdata-cdh01.itcast.cn",
      "2181", //
      "tbl_users",
      "info",
      "id"
    )

    spark.stop()
  }
}

