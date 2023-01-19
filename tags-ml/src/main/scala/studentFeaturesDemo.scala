import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, LogisticRegressionSummary}
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取数据集,封装特征至Feature和标签处理label中
 *
 * TODO 全部基于DataFram API 实现
 */
object studentFeaturesDemo {
  def main(args: Array[String]): Unit = {
    //构建sparksession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()


    //TODO 1: 加载数据集data2.csv
    val schema: StructType = StructType(
      Array(
        StructField("Index", DoubleType, nullable = true),
        StructField("CardNo", DoubleType, nullable = true),
        StructField("PeoNo", DoubleType, nullable = true),
        StructField("Date", StringType, nullable = true),
        StructField("Money", DoubleType, nullable = true),
        StructField("FundMoney", DoubleType, nullable = true),
        StructField("Surplus", DoubleType, nullable = true),
        StructField("CardCount", DoubleType, nullable = true),
        StructField("Type", StringType, nullable = true),
        StructField("TermNo", DoubleType, nullable = true),
        StructField("TermSerNo", StringType, nullable = true),
        StructField("conOperNo", StringType, nullable = true),
        StructField("OperNo", DoubleType, nullable = true),
        StructField("Dept", StringType, nullable = true)
      )
    )
    val datas: DataFrame = spark.read
      .option("seq", ",")
      //当csv文件首行不是列名称时最好自定义 schema
      .option("header", "true")
      .option("inferSchema", "false")
      .option("encoding", "GBK")
      .schema(schema)
      .csv("datas/data2.csv")
    println("==========================================读取=========================================")
    //    datas.printSchema()
    //    datas.show()

    /*
    root
       |-- Index: double (nullable = true)
       |-- CardNo: double (nullable = true)
       |-- PeoNo: double (nullable = true)
       |-- Date: string (nullable = true)
       |-- Money: double (nullable = true)
       |-- FundMoney: double (nullable = true)
       |-- Surplus: double (nullable = true)
       |-- CardCount: double (nullable = true)
       |-- Type: string (nullable = true)
       |-- TermNo: double (nullable = true)
       |-- TermSerNo: string (nullable = true)
       |-- conOperNo: string (nullable = true)
       |-- OperNo: double (nullable = true)
       |-- Dept: string (nullable = true)
     */

    //TODO step1 -> 将特征封装至特征(feature)向量中
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("CardNo", "PeoNo", "Money", "FundMoney", "Surplus", "CardCount", "TermNo", "OperNo"))
      .setOutputCol("features")
    val output = assembler.transform(datas)
    //        output.printSchema()
    //        output.show()

    //TODO step2 -> 转换类别字符串数拊为数值数据
    val indexer = new StringIndexer()
      .setInputCol("Dept")
      .setOutputCol("label")
    val indexed = indexer
      .fit(output)
      .transform(output)
    println("==========================================特征封装与label转换=========================================")
    //    indexed.printSchema()
    //    indexed.show()

    //TODO step3 -> 将特征数据features标准化处理
    /*
          机器学习核心三要素: 数据(特征features)＋算法=模型(最佳)
          调优中，最重要的就是特征数据features,如果特征数据比较好,处理恰当,可能得到较好模型
          TODO: 在实际开发中，特征数据features需要进行各个转换操作，比如正则化、归一化或标准化等等
          不同维度特征值，值的范围跨度不一样,导致模型异常
     */
    val scaler: StandardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scale_Features")
      .setWithStd(true) //使用标准差缩放
      .setWithMean(false) //不使用平均值缩放
    //Compute summary statistics by fitting the StandardScaler.
    val rsDF = scaler
      .fit(indexed)
      .transform(indexed)
    println("==========================================标准化=========================================")
    //    rsDF.show()
    //    rsDF.printSchema()

    //TODO: 2.选择分类算法,构建分类模型
    /**
     * 分类算法属于最多的算法,比如:
     * 1. 决策树分类算法
     * 2. 朴素贝叶斯算法,适合构建文本数据特征分类,比如垃圾邮件,情感分析
     * 3. 逻辑回归算法
     * 4. 线性支持向量机分类算法
     * 5. 神经网络相关分类算法，比如多层感知肌算法 --> 深度学习算法
     * 6. 集成融合算法:随机森林算法(RF算法)、梯度提升树(GBT算法)
     *
     * 线性回归算法
     * -最小二乘法
     * -梯度下降法
     * -牛顿迭代法
     */
    val student: LogisticRegression = new LogisticRegression()
      //设置特征值的列名称与标签值的列名称
      .setFeaturesCol("scale_Features") //x -> 特征
      .setLabelCol("label") //y -> 标签
      //每个算法都有自己的超参数设置,比较关键,合理的设置会获得较好的模型
      .setMaxIter(30) //最大迭代次数
      .setFamily("multinomial") //设置分类属于二分类还是多分类(就是标签label有多少个值)
      .setStandardization(true) //是否对特征数据进行标准化
      .setRegParam(0) //正则化参数,优化
      .setElasticNetParam(0) //弹性化参数,优化

    //TODO: 将数据运用于算法中,训练模型
    val studentModel: LogisticRegressionModel = student.fit(rsDF)


    //TODO: 评估模型
    println("==========================================模型评估=========================================")
    println(s"多分类混淆矩阵:${studentModel.coefficientMatrix}")
    val summary: LogisticRegressionSummary = studentModel.summary
    println(s"accuracy:${summary.accuracy}") //模型准确度%
    println(s"accuracy:${summary.precisionByLabel.mkString(",")}") //每个标签的精度


    studentModel.save("datas/student_model")

    //应用结束,关闭资源你
    spark.close
  }
}
