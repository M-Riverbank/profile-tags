package cn.itcast.tags.ml.classification

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 基于泰塔尼克号数据集，使用逻辑回归构建分类模型，评估模型
 */
object TitanicLrClassification {
  def main(args: Array[String]): Unit = {
    //构建sparksession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //TODO: 1.加载数据，数据过滤与基本转换
    val TitanicDF: DataFrame = spark
      .read
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv("datas/titanic/train.csv")
    //    TitanicDF.printSchema()
    //    TitanicDF.show()
    /*
        root
           |-- PassengerId: integer (nullable = true)
           |-- Survived: integer (nullable = true)
           |-- Pclass: integer (nullable = true)
           |-- Name: string (nullable = true)
           |-- Sex: string (nullable = true)
           |-- Age: double (nullable = true)
           |-- SibSp: integer (nullable = true)
           |-- Parch: integer (nullable = true)
           |-- Ticket: string (nullable = true)
           |-- Fare: double (nullable = true)
           |-- Cabin: string (nullable = true)
           |-- Embarked: string (nullable = true)
     */

    /*
        监督学习
            label属于离散值使用分类算法
                    分类算法属于最多的算法,比如:
                    1. 决策树分类算法
                    2. 朴素贝叶斯算法,适合构建文本数据特征分类,比如垃圾邮件,情感分析
                    3. 逻辑回归算法
                    4. 线性支持向量机分类算法
                    5. 神经网络相关分类算法，比如多层感知肌算法 --> 深度学习算法
                    6. 集成融合算法:随机森林算法(RF算法)、梯度提升树(GBT算法)
            label属于连续值使用回归算法
                    线性回归算法(代价函数j(θ)求最小)
                    -最小二乘法（矩阵相乘,交替最小二乘法(ALS):多用于推荐系统）RDD
                    -梯度下降法(求导微积分)RDD
                    -牛顿迭代法(泰勒公式)DataFrame
    */
    //TODO: 2.数据准备:特征工程（提取·转换与选择)
    //2.1 Age年龄字段有缺省值，填充为年龄字段平均值
    val avgAge: Double = TitanicDF //提取平均年龄
      .select("Age")
      .filter($"Age".isNotNull)
      .select(round(avg($"Age"), 2).as("avgAge"))
      .first()
      .getAs[Double]("avgAge")
    val ageTitanicDF: DataFrame = TitanicDF
      .select(
        //标签label
        $"Survived".as("label"),
        //特征features
        $"Pclass",
        $"Sex",
        $"SibSp",
        $"Parch",
        $"Fare",
        $"Age",
        //当年龄为null时使用 avgAge 代替,否则为$"Age"值
        when($"Age".isNotNull, $"Age").otherwise(avgAge).as("defaultAge")
      )
    // 2.2 对Sex字段类别特征换换，使用StringIndexer和OneHotEncoder
    // male ->0  ,female -> 1
    val indexer: StringIndexer = new StringIndexer()
      .setInputCol("Sex")
      .setOutputCol("sexIndex")
    val indexerTitanicDF = indexer
      .fit(ageTitanicDF)
      .transform(ageTitanicDF)
    // male -> [1.0, 0.0]    female -> [0.0, 1.0]
    val encoder: OneHotEncoder = new OneHotEncoder()
      .setInputCol("sexIndex")
      .setOutputCol("sexVector")
      .setDropLast(false)
    val sexTitanicDF: DataFrame = encoder.transform(indexerTitanicDF)
    // 2.3 将特征值组合, 使用VectorAssembler
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(
        Array("Pclass", "sexVector", "SibSp", "Parch", "Fare", "defaultAge")
      )
      .setOutputCol("features")
    val titanicDF: DataFrame = assembler.transform(sexTitanicDF)
    //    titanicDF.printSchema()
    //    titanicDF.show(20, truncate = false)

    // 2.4 划分数据集为训练集和测试集
    val Array(trainingDF, testingDF) = titanicDF.randomSplit(Array(0.8, 0.2))
    trainingDF.cache().count()

    // TODO: 3. 使用算法和数据构建模型：算法参数
    val logisticRegression: LogisticRegression = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction") // 使用模型预测时，预测值的列名称
      // 二分类
      .setFamily("binomial")
      .setStandardization(true)
      // 超参数
      .setMaxIter(100)
      .setRegParam(0.1)
      .setElasticNetParam(0.8)
    val lrModel: LogisticRegressionModel = logisticRegression.fit(trainingDF)
    // y = θ0 + θ1x1+ θ2x2+ θ3x4+ θ4x4+ θ5x6+ θ6x6
    println(s"coefficients: ${lrModel.coefficientMatrix}") // 斜率, θ1 ~ θ6
    println(s"intercepts: ${lrModel.interceptVector}") // 截距, θ0

    // TODO: 4. 模型评估
    val predictionDF: DataFrame = lrModel.transform(testingDF)
    predictionDF.printSchema()
    predictionDF
      .select("label", "prediction", "probability", "features")
      .show(40, truncate = false)
    // 分类中的ACCU、Precision、Recall、F-measure、Accuracy
    val accuracy = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      // 四个指标名称："f1", "weightedPrecision", "weightedRecall", "accuracy"
      .setMetricName("accuracy")
      .evaluate(predictionDF)
    println(s"accuracy = $accuracy")

    spark.stop()
  }
}
