
package edu.gatech.cse8803.ml_models

import edu.gatech.cse8803.main.Main.{KeyTuple, ValueTuple}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


class RandomForest {

  var pipeline: PipelineModel = _
  var sqlContext: SQLContext = _

  def train(data: DataFrame, numTrees: Int = 25): PipelineModel = {
    val labelIndexer = new StringIndexer().setInputCol("label").
                                           setOutputCol("indexedLabel").
                                           fit(training)
    val rf = new RandomForestClassifier().setLabelCol("indexedLabel").
                                          setNumTrees(numTrees)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, rf))
    this.pipeline = pipeline.fit(data)
    this.pipeline
  }

  def predict(data: DataFrame): DataFrame = {
    this.pipeline.transform(data)
  }

  def getAUC(predictions: DataFrame): Double = {
    val binEval = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").
                                                      setRawPredictionCol("rawPrediction")
    binEval.setMetricName("areaUnderROC").evaluate(predictions)
  }

  def getAUC(data: RDD[(KeyTuple, ValueTuple)]): Double ={
    val predictions = predict(data)
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
