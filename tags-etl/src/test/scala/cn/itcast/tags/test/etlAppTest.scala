package cn.itcast.tags.test

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.util.Random

object etlAppTest {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    // 模拟数据
    val ordersDF: DataFrame = Seq(
      "1572969600" // 2019-07-29 23:43:42
    ).toDF("finishtime")

    val data: DataFrame = ordersDF
      .withColumn(
        "finishTime", //
        unix_timestamp(
          date_add(from_unixtime($"finishTime"), 350), //
          "yyyy-MM-dd"
        )
      ).select($"finishTime")
    data.printSchema()
    data.show()


    val finishTime_udf: UserDefinedFunction = udf(
      (finishTime: String) => {
        System.currentTimeMillis() - new Random().nextInt(15552000)
      }
    )

    val data2: DataFrame = ordersDF
      .withColumn(
        "finishTime", //
        finishTime_udf($"finishTime")
      ).select($"finishTime")

    data2.printSchema()
    data2.show()
  }
}
