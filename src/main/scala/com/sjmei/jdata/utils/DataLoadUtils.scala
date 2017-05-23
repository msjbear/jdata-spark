package com.sjmei.jdata.utils

import org.apache.spark.ml.feature.{Normalizer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._

/**
  * Created by cdmeishangjian on 2017/5/7.
  */
object DataLoadUtils {

  val sep = AlgoUtils.FIELD_SEP
  val numPartitions = AlgoUtils.NUM_PARTITIONS

  /**
    * load xgboost model train dataset
    *
    * @param spark
    * @param input
    * @param fracRatio
    * @return
    */
  def loadTrainDataOrc(spark: SparkSession, input: String,
                       fracRatio: Double): (DataFrame, DataFrame) = {

    // Load training data
    val origExamples: DataFrame = loadDataOrc(spark, input, sep)

    // Split input into training, test.
    val dataframes = origExamples.randomSplit(Array(1 - fracRatio, fracRatio), seed = 12345)

    val training = dataframes(0).cache()
    val test = dataframes(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    val numFeatures = training.select("features").first().size
    println("Loaded data:")
    println(s"  numTraining = $numTraining, numTest = $numTest")
    println(s"  numFeatures = $numFeatures")
    training.show(10)
    test.show(10)

    (training, test)
  }

  def loadTrainData(spark: SparkSession, input: String,
                    fracRatio: Double): (DataFrame, DataFrame) = {

    // Load training data
    val origExamples: DataFrame = loadData(spark, input, sep)

    // Split input into training, test.
    val dataframes = origExamples.randomSplit(Array(1 - fracRatio, fracRatio), seed = 12345)

    val training = dataframes(0).cache()
    val test = dataframes(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    val numFeatures = training.select("features").first().size
    println("Loaded data:")
    println(s"  numTraining = $numTraining, numTest = $numTest")
    println(s"  numFeatures = $numFeatures")
    training.show(10)
    test.show(10)

    (training, test)
  }

  def loadUserTrainData(spark: SparkSession, input: String,
                    fracRatio: Double): (DataFrame, DataFrame) = {

    // Load training data
    val origExamples: DataFrame = loadUserData(spark, input, sep)

    // Split input into training, test.
    val dataframes = origExamples.randomSplit(Array(1 - fracRatio, fracRatio), seed = 12345)

    val training = dataframes(0).cache()
    val test = dataframes(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    val numFeatures = training.select("features").first().size
    println("Loaded data:")
    println(s"  numTraining = $numTraining, numTest = $numTest")
    println(s"  numFeatures = $numFeatures")
    training.show(10)
    test.show(10)

    (training, test)
  }

  def genTrainDataFromHive(spark: SparkSession, tableSuffix: String, initDate: String,
                    fracRatio: Double): (DataFrame, DataFrame) = {

    // Load training data
    val user_dim_feature_script = VelocityUtils.genDimFeatureSql("dim_user_feature_etl.sql", tableSuffix, initDate)
    val sku_dim_feature_script = VelocityUtils.genDimFeatureSql("dim_sku_feature_etl.sql", tableSuffix, initDate)
    val brand_dim_feature_script = VelocityUtils.genDimFeatureSql("dim_brand_feature_etl.sql", tableSuffix, initDate)
    val user_brand_dim_feature_script = VelocityUtils.genDimFeatureSql("dim_user_brand_feature_etl.sql", tableSuffix, initDate)
    val user_sku_dim_feature_script = VelocityUtils.genDimFeatureSql("dim_user_sku_feature_etl.sql", tableSuffix, initDate)

    val df_user_feature = spark.sql(user_dim_feature_script)
    df_user_feature.createOrReplaceTempView("feature_user_dim")

    val df_sku_feature = spark.sql(sku_dim_feature_script)
    df_sku_feature.createOrReplaceTempView("feature_sku_dim")

    val df_brand_feature = spark.sql(brand_dim_feature_script)
    df_brand_feature.createOrReplaceTempView("feature_brand_dim")

    val df_user_brand_feature =  spark.sql(user_brand_dim_feature_script)
    df_user_brand_feature.createOrReplaceTempView("feature_user_brand_dim")

    val df_user_sku_feature = spark.sql(user_sku_dim_feature_script)
    df_user_sku_feature.createOrReplaceTempView("feature_user_sku_dim")
    println("JDATA: dim feature run finished.")

    val feature_wide_table_script = VelocityUtils.genDimFeatureSql("feature_wide_table.sql", tableSuffix, initDate)
    val trainData: DataFrame = spark.sql(feature_wide_table_script)

    val origExamples: DataFrame = loadDataFromDF(spark, trainData)

    // Split input into training, test.
    val dataframes = origExamples.randomSplit(Array(1 - fracRatio, fracRatio), seed = 12345)

    val training = dataframes(0).cache()
    val test = dataframes(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    val numFeatures = training.select("features").first().size
    println("Loaded data:")
    println(s"  numTraining = $numTraining, numTest = $numTest")
    println(s"  numFeatures = $numFeatures")
    training.show(10)
    test.show(10)

    df_user_feature.unpersist(blocking = false)
    df_sku_feature.unpersist(blocking = false)
    df_brand_feature.unpersist(blocking = false)
    df_user_brand_feature.unpersist(blocking = false)
    df_user_sku_feature.unpersist(blocking = false)
    trainData.unpersist(blocking = false)
    origExamples.unpersist(blocking = false)


    (training, test)
  }

  /**
    * load xgboost model predict dataset
    *
    * @param spark
    * @param path
    * @return
    */
  def loadPredictDataOrc(spark: SparkSession, path: String): DataFrame = {

    import spark.implicits._

    val df = spark.read.orc(path).repartition(numPartitions).map(line => {
      val tokens = line.mkString(sep).split(sep, -1)
      val features = Vectors.dense(tokens.drop(3).map(toDouble(_)))

      (tokens(0), tokens(1), features)
    }).toDF("user_id", "sku_id", "raw_features")

    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("index_features")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(df)
    val indexedData = indexerModel.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("index_features")
      .setOutputCol("features")
      .setP(1.0)
    val l1NormData = normalizer.transform(indexedData)

    l1NormData
  }

  def loadUserPredDataOrc(spark: SparkSession, path: String): DataFrame = {

    import spark.implicits._

    val df = spark.read.orc(path).repartition(numPartitions).map(line => {
      val tokens = line.mkString(sep).split(sep, -1)
      val features = Vectors.dense(tokens.drop(1).map(toDouble(_)))

      (tokens(0), features)
    }).toDF("user_id", "raw_features")

    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("index_features")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(df)
    val indexedData = indexerModel.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("index_features")
      .setOutputCol("features")
      .setP(1.0)
    val l1NormData = normalizer.transform(indexedData)

    l1NormData
  }

  def loadPredictData(spark: SparkSession, path: String): DataFrame = {

    import spark.implicits._

    val df = spark.read.text(path).repartition(numPartitions).map(line => {
      val tokens = line.mkString(sep).split(sep, -1)
      val features = Vectors.dense(tokens.drop(3).map(toDouble(_)))

      (tokens(0), tokens(1), features)
    }).toDF("user_id", "sku_id", "raw_features")

    // (2) Identify categorical features using VectorIndexer.
    // Features with more than maxCategories values will be treated as continuous.
    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("index_features")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(df)
    val indexedData = indexerModel.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("index_features")
      .setOutputCol("features")
      .setP(1.0)
    val l1NormData = normalizer.transform(indexedData)

    l1NormData
  }

  /**
    * load xgboost model eval dataset
    *
    * @param spark
    * @param input
    * @return
    */
  def loadEvalDataOrc(spark: SparkSession, input: String): DataFrame = {

    // Load training data
    val dataDF: DataFrame = loadDataOrc(spark, input, sep)

    val numTraining = dataDF.count()
    println("Loaded data:")
    println(s"  numTraining = $numTraining")
    dataDF.show(10)

    dataDF
  }

  def loadEvalData(spark: SparkSession, input: String): DataFrame = {

    // Load training data
    val dataDF: DataFrame = loadData(spark, input, sep)

    val numTraining = dataDF.count()
    println("Loaded data:")
    println(s"  numTraining = $numTraining")
    dataDF.show(10)

    dataDF
  }

  def loadUserEvalData(spark: SparkSession, input: String): DataFrame = {

    // Load training data
    val dataDF: DataFrame = loadUserData(spark, input, sep)

    val numTraining = dataDF.count()
    println("Loaded data:")
    println(s"  numTraining = $numTraining")
    dataDF.show(10)

    dataDF
  }

  def loadDataOrc(spark: SparkSession, path: String, sep: String): DataFrame = {

    import spark.implicits._

    val df = spark.read.orc(path).repartition(numPartitions).map(line => {
      val tokens = line.mkString(sep).split(sep, -1)
      val features = Vectors.dense(tokens.drop(4).map(toDouble(_)))
      if (tokens(3).equalsIgnoreCase("1")) {
        (tokens(0), tokens(1), 1, features)
      } else {
        (tokens(0), tokens(1), 0, features)
      }
    }).toDF("user_id", "sku_id", "label", "raw_features")

    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("index_features")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(df)
    val indexedData = indexerModel.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("index_features")
      .setOutputCol("features")
      .setP(1.0)
    val l1NormData = normalizer.transform(indexedData)

    l1NormData
  }


  def loadData(spark: SparkSession, path: String, sep: String = sep): DataFrame = {

    import spark.implicits._

    val df = spark.read.text(path).repartition(numPartitions).map(line => {
      val tokens = line.mkString(sep).split(sep, -1)
      val features = Vectors.dense(tokens.drop(4).map(toDouble(_)))
      if (tokens(3).equalsIgnoreCase("1")) {
        (tokens(0), tokens(1), 1, features)
      } else {
        (tokens(0), tokens(1), 0, features)
      }
    }).toDF("user_id", "sku_id", "label", "raw_features")

    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("index_features")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(df)
    val indexedData = indexerModel.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("index_features")
      .setOutputCol("features")
      .setP(1.0)
    val l1NormData = normalizer.transform(indexedData)

    l1NormData
  }

  def loadUserData(spark: SparkSession, path: String, sep: String = sep): DataFrame = {

    import spark.implicits._

    val df = spark.read.text(path).repartition(numPartitions).map(line => {
      val tokens = line.mkString(sep).split(sep, -1)
      val features = Vectors.dense(tokens.drop(2).map(toDouble(_)))
      if (tokens(1).equalsIgnoreCase("1")) {
        (tokens(0), 1, features)
      } else {
        (tokens(0), 0, features)
      }
    }).toDF("user_id", "label", "raw_features")

    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("index_features")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(df)
    val indexedData = indexerModel.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("index_features")
      .setOutputCol("features")
      .setP(1.0)
    val l1NormData = normalizer.transform(indexedData)

    l1NormData
  }

  def loadDataFromDF(spark: SparkSession, trainData: DataFrame): DataFrame = {

    import spark.implicits._

    val df = trainData.repartition(numPartitions).map(line => {
      val tokens = line.mkString(sep).split(sep, -1)
      val features = Vectors.dense(tokens.drop(4).map(toDouble(_)))
      if (tokens(3).equalsIgnoreCase("1")) {
        (tokens(0), tokens(1), 1, features)
      } else {
        (tokens(0), tokens(1), 0, features)
      }
    }).toDF("user_id", "sku_id", "label", "raw_features")

    val indexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("index_features")
      .setMaxCategories(10)
    val indexerModel = indexer.fit(df)
    val indexedData = indexerModel.transform(df)

    val normalizer = new Normalizer()
      .setInputCol("index_features")
      .setOutputCol("features")
      .setP(1.0)
    val l1NormData = normalizer.transform(indexedData)

    l1NormData
  }

  def toDouble(s:String):Double= {
    try{
      s.trim.toDouble
    } catch {
      case e: Exception => 0.0
    }
  }

}
