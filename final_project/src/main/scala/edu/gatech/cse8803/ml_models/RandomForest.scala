
package edu.gatech.cse8803.ml_models

import edu.gatech.cse8803.main.Main.{KeyTuple, ValueTuple}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions.monotonicallyIncreasingId

class RandomForest {

  var pipeline: PipelineModel = _
  var sqlContext: SQLContext = _
  //cache predictions
  var prevDataFrame: String = "0x0"
  var prevPredictions: DataFrame = _

  def train(data: DataFrame, numTrees: Int = 2): PipelineModel = {
    val labelIndexer = new StringIndexer().setInputCol("label").
                                           setOutputCol("indexedLabel").
                                           fit(data)
    val rf = new RandomForestClassifier().setLabelCol("indexedLabel").
                                          setNumTrees(numTrees)


    val pipeline = new Pipeline().setStages(Array(labelIndexer, rf))
    this.pipeline = pipeline.fit(data)
    this.pipeline
  }

  def predict(data: DataFrame): DataFrame = {
    if (data.toString() == prevDataFrame) {
      prevPredictions
    } else {
      prevDataFrame = data.toString()
      prevPredictions = this.pipeline.transform(data).select("indexedLabel", "prediction", "rawPrediction")
      prevPredictions
    }
  }

  def getEvaluationMetrics(): RDD[(Int, Int)] = {
    if (prevDataFrame != "0x0") {
      val confusionMatrix = prevPredictions.map({
        case row => if (row(0).toString.toDouble == 0.0 && row(1).toString.toDouble == 0.0){
          (0, 1)
        } else if (row(0).toString.toDouble == 0.0 && row(1).toString.toDouble == 1.0) {
          (-1, 1)
        } else if (row(0).toString.toDouble == 1.0 && row(1).toString.toDouble == 1.0) {
          (1, 1)
        } else {
          (-2, 1)
        }
      }).reduceByKey(_+_)
      confusionMatrix
    } else {
      val sc = SparkContext.getOrCreate()
      sc.emptyRDD[(Int, Int)]
    }
  }

  def getEvaluationMetrics(data: DataFrame): Unit = {
    val preds = {
      if (prevDataFrame != data.toString()) {
        predict(data)
      } else {
        prevPredictions
      }
    }
    val confusionMatrix = preds.map({
      case row => if (row(0).toString.toDouble == 0.0 && row(1).toString.toDouble == 0.0){
        (0, 1)
      } else if (row(0).toString.toDouble == 0.0 && row(1).toString.toDouble == 1.0) {
        (-1, 1)
      } else if (row(0).toString.toDouble == 1.0 && row(1).toString.toDouble == 1.0) {
        (1, 1)
      } else {
        (-2, 1)
      }
    }).reduceByKey(_+_)
    println(confusionMatrix.take(4))
  }

  def getAUC(data: DataFrame, givenPredictions: Boolean = false): Double = {
    val predictions = if (givenPredictions) data else predict(data)
    val binEval = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").
                                                      setRawPredictionCol("rawPrediction")
    binEval.setMetricName("areaUnderROC").evaluate(predictions)
  }

  def convertRDDtoDF(data: RDD[(KeyTuple, ValueTuple)]) = {
    this.sqlContext = new SQLContext(data.context)
    val segmented = data.map({
      case (k,v) => (v._2.toDouble, Vectors.dense(v._3.bpDia, v._3.bpSys, v._3.heartRate,
                     v._3.respRate, v._3.temp, v._3.spo2, v._3.gcs, v._3.age.toDouble))
    })
    this.sqlContext.createDataFrame(segmented).toDF("label", "features")
  }
}
