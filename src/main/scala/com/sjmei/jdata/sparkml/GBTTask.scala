/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.sjmei.jdata.sparkml

import com.sjmei.jdata.utils.{AlgoUtils, DataLoadUtils, SubmissionEvalUtils}
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.utils.Logging
import scopt.OptionParser

import scala.collection.mutable
import scala.language.reflectiveCalls


/**
  *
  * Created by cdmeishangjian on 2016/10/26.
  *
 * An example runner for decision trees. Run with
 * {{{
 * ./bin/run-example ml.GBTExample [options]
 * }}}
 * Decision Trees and ensembles can take a large amount of memory. If the run-example command
 * above fails, try running via spark-submit and specifying the amount of memory as at least 1g.
 * For local mode, run
 * {{{
 * ./bin/spark-submit --class org.apache.spark.examples.ml.GBTExample --driver-memory 1g
 *   [examples JAR path] [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object GBTTask extends Logging {

  case class Params(
      input: String = null,
      modelDir: String = null,
      output: String = null,
      taskType:String = null,
      initDate: String = "2016-04-06",
      dataFormat: String = "libsvm",
      algo: String = "classification",
      maxDepth: Int = 10,
      maxBins: Int = 50,
      minInstancesPerNode: Int = 1,
      minInfoGain: Double = 0.0,
      maxIter: Int = 100,
      fracTest: Double = 0.1,
      isCvModel: Boolean = false,
      cacheNodeIds: Boolean = false,
      checkpointDir: Option[String] = None,
      checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("GBTExample") {
      head("GBTExample: an example Gradient-Boosted Trees app.")
      opt[String]("initDate")
        .text("initDate of predict")
        .action((x, c) => c.copy(initDate = x))
      opt[String]("algo")
        .text(s"algorithm (classification, regression), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = x))
      opt[Int]("maxDepth")
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[Int]("minInstancesPerNode")
        .text(s"min number of instances required at child nodes to create the parent split," +
        s" default: ${defaultParams.minInstancesPerNode}")
        .action((x, c) => c.copy(minInstancesPerNode = x))
      opt[Double]("minInfoGain")
        .text(s"min info gain required to create a split, default: ${defaultParams.minInfoGain}")
        .action((x, c) => c.copy(minInfoGain = x))
      opt[Int]("maxIter")
        .text(s"number of trees in ensemble, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing. If given option testInput, " +
        s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[Boolean]("isCvModel")
        .text("is cvmodel flag: false (default)")
        .action((x, c) => c.copy(isCvModel = x))
      opt[Boolean]("cacheNodeIds")
        .text(s"whether to use node Id cache during training, " +
        s"default: ${defaultParams.cacheNodeIds}")
        .action((x, c) => c.copy(cacheNodeIds = x))
      opt[String]("checkpointDir")
        .text(s"checkpoint directory where intermediate node Id caches will be stored, " +
        s"default: ${
          defaultParams.checkpointDir match {
            case Some(strVal) => strVal
            case None => "None"
          }
        }")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"how often to checkpoint the node Id cache, " +
        s"default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      opt[String]("dataFormat")
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
        .action((x, c) => c.copy(dataFormat = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      arg[String]("<modelDir>")
        .text("modelDir path to labeled examples")
        .required()
        .action((x, c) => c.copy(modelDir = x))
      arg[String]("<output>")
        .text("output path to labeled examples")
        .required()
        .action((x, c) => c.copy(output = x))
      arg[String]("<taskType>")
        .text("train or predict the rf model")
        .required()
        .action((x, c) => c.copy(taskType = x))
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => {
        params.taskType.trim.toLowerCase match {
          case "train" => train(params)
          case "train_cv" => train_cv(params)
          case "predict" => predict(params)
          case "predict_eval" => predict_eval(params)
          case _ => println("XGBoost method error...")
        }
      }
      case _ => sys.exit(1)
    }
  }

  def train(params: Params): Unit = {
    val spark = AlgoUtils.getSparkSession(s"GBTExample with $params")

    params.checkpointDir.foreach(spark.sparkContext.setCheckpointDir)
    val algo = params.algo.toLowerCase

    println(s"GBTExample with parameters:\n$params")

    // Load training and test data and cache it.
    val (training: DataFrame, test: DataFrame) = DataLoadUtils.loadTrainData(spark, params.input, params.fracTest)

    // Set up Pipeline
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    // Learn GBT.
    val dt = algo match {
      case "classification" =>
        new GBTClassifier()
          .setFeaturesCol("features")
          .setLabelCol("label")
          .setMaxDepth(params.maxDepth)
          .setMaxBins(params.maxBins)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setCacheNodeIds(params.cacheNodeIds)
          .setCheckpointInterval(params.checkpointInterval)
          .setMaxIter(params.maxIter)
      case "regression" =>
        new GBTRegressor()
          .setFeaturesCol("features")
          .setLabelCol("label")
          .setMaxDepth(params.maxDepth)
          .setMaxBins(params.maxBins)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setCacheNodeIds(params.cacheNodeIds)
          .setCheckpointInterval(params.checkpointInterval)
          .setMaxIter(params.maxIter)
      case _ => throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }
    stages += dt
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipelineModel = pipeline.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val gbtModel = pipelineModel.stages.last.asInstanceOf[GBTClassificationModel]
    gbtModel.write.overwrite.save(params.modelDir)

    val predictions = pipelineModel.transform(training)
    val df_test_pred = pipelineModel.transform(test)
    // Get the trained GBT from the fitted PipelineModel.
    algo match {
      case "classification" =>
        if (gbtModel.totalNumNodes < 30) {
          println(gbtModel.toDebugString) // Print full model.
        } else {
          println(gbtModel) // Print model summary.
        }
      case "regression" =>
        val gbtModel = pipelineModel.stages.last.asInstanceOf[GBTRegressionModel]
        if (gbtModel.totalNumNodes < 30) {
          println(gbtModel.toDebugString) // Print full model.
        } else {
          println(gbtModel) // Print model summary.
        }
      case _ => throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }

    // Evaluate model on training, test data.
    algo match {
      case "classification" =>
        println("Training & Testing data evaluate results:")
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val train_accuracy = evaluator.evaluate(predictions)
        val test_accuracy = evaluator.evaluate(df_test_pred)
        println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")
      case "regression" =>
        println("Training & Testing data evaluate results:")
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val train_accuracy = evaluator.evaluate(predictions)
        val test_accuracy = evaluator.evaluate(df_test_pred)
        println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")
      case _ =>
        throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }

    AlgoUtils.saveNormProbResult(spark, predictions, params.output)
    predictions.printSchema()
    predictions.select("prediction","rawPrediction","probability", "label", "features").show(5)


    spark.stop()
  }


  def train_cv(params: Params): Unit = {

    val spark = AlgoUtils.getSparkSession(s"GBTExample with $params")

    params.checkpointDir.foreach(spark.sparkContext.setCheckpointDir)
    val algo = params.algo.toLowerCase

    println(s"GBTExample with parameters:\n$params")

    // Load training and test data and cache it.
    val (training: DataFrame, test: DataFrame) = DataLoadUtils.loadTrainData(spark, params.input, params.fracTest)

    // Set up Pipeline.
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    // Learn GBT.
    val dt = algo match {
      case "classification" =>
        new GBTClassifier()
          .setFeaturesCol("features")
          .setLabelCol("label")
          .setMaxDepth(params.maxDepth)
          .setMaxBins(params.maxBins)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setCacheNodeIds(params.cacheNodeIds)
          .setCheckpointInterval(params.checkpointInterval)
          .setMaxIter(params.maxIter)
      case "regression" =>
        new GBTRegressor()
          .setFeaturesCol("features")
          .setLabelCol("label")
          .setMaxDepth(params.maxDepth)
          .setMaxBins(params.maxBins)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setCacheNodeIds(params.cacheNodeIds)
          .setCheckpointInterval(params.checkpointInterval)
          .setMaxIter(params.maxIter)
      case _ => throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }
    stages += dt
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline.
    val startTime = System.nanoTime()

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(8, 10))
      .addGrid(dt.maxIter, Array(50, 100, 200))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(training)
    cvModel.write.overwrite.save(params.modelDir)
    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val train_predict = cvModel.transform(training).select("label","prediction","probability")
    val test_predict = cvModel.transform(test).select("label","prediction","probability")
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    val train_accuracy = evaluator.evaluate(train_predict)
    val test_accuracy = evaluator.evaluate(test_predict)
    println(s"Train Accuracy = $train_accuracy,  Test Accuracy = $test_accuracy")
    // $example off$

    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    train_predict.printSchema()
    train_predict.select("label","prediction","probability").show(10)

    spark.stop()
  }


  def predict(params: Params): Unit = {
    val spark = AlgoUtils.getSparkSession(s"GBTExample with $params")

    params.checkpointDir.foreach(spark.sparkContext.setCheckpointDir)

    println(s"GBTExample with parameters:\n$params")

    // Load training and test data and cache it.
    val datasets = DataLoadUtils.loadPredictDataOrc(spark,params.input)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    var results: DataFrame = null
    if(params.isCvModel){
      val predModel = CrossValidatorModel.load(params.modelDir)
      results = predModel.transform(datasets)

    }else{
      val predModel = GBTClassificationModel.load(params.modelDir)
      results = predModel.transform(datasets)
    }

    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    AlgoUtils.saveNormProbResult(spark, results, params.output)

    import spark.implicits._
    val predicts = results.select("user_id", "sku_id", "probability", "prediction")
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[DenseVector].toArray(1),
        row.get(3).asInstanceOf[Double])
      }).toDF("user_id", "sku_id", "prob", "predict")

    predicts.createOrReplaceTempView("future_predict_table")

    val script_sql = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)

    import spark.implicits._
    val submission_result = spark.sql(script_sql)
    submission_result.map(_.mkString(AlgoUtils.FIELD_SEP)).write.mode(SaveMode.Overwrite).text(params.output +".submit")
    println("JRDM: submission cnt: " + submission_result.count())

    datasets.unpersist(blocking = false)
    results.unpersist(blocking = false)
    predicts.unpersist(blocking = false)
    submission_result.unpersist(blocking = false)

    spark.stop()
  }


  def predict_eval(params: Params): Unit = {
    val spark = AlgoUtils.getSparkSession(s"GBTExample with $params")

    params.checkpointDir.foreach(spark.sparkContext.setCheckpointDir)

    println(s"GBTExample with parameters:\n$params")

    // Load training and test data and cache it.
    val datasets = DataLoadUtils.loadEvalDataOrc(spark, params.input)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    var results: DataFrame = null
    if(params.isCvModel){
      val predModel = CrossValidatorModel.load(params.modelDir)
      results = predModel.transform(datasets)

    }else{
      val predModel = GBTClassificationModel.load(params.modelDir)
      results = predModel.transform(datasets)
    }
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    AlgoUtils.saveEvalNormProbResult(spark, results, params.output)

    import spark.implicits._
    val predicts = results.select("user_id", "sku_id", "label", "probability", "prediction")
      .map(row => {(row.get(0).asInstanceOf[String],
        row.get(1).asInstanceOf[String],
        row.get(2).asInstanceOf[Int],
        row.get(3).asInstanceOf[DenseVector].toArray(1),
        row.get(4).asInstanceOf[Double])
      }).toDF("user_id", "sku_id", "label", "prob", "predict")

    predicts.createOrReplaceTempView("future_predict_table")

    val script_sql = AlgoUtils.genSubmissionResultSql("gen_submission_result.sql", params.initDate)

    import spark.implicits._
    val submission_result = spark.sql(script_sql)
    submission_result.map(_.mkString(AlgoUtils.FIELD_SEP)).write.mode(SaveMode.Overwrite).text(params.output +".submit")
    println("JRDM: submission cnt: " + submission_result.count())

    results.cache()
    submission_result.cache()

    SubmissionEvalUtils.jdata_report(predicts, submission_result)

    datasets.unpersist(blocking = false)
    results.unpersist(blocking = false)
    predicts.unpersist(blocking = false)
    submission_result.unpersist(blocking = false)


    spark.stop()
  }
}
// scalastyle:on println
