
package edu.gatech.cse8803.ml_models

import edu.gatech.cse8803.main.Main.{KeyTuple, ValueTuple}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint

class SVM {


  var sqlContext: SQLContext = _
  var svmm: SVMModel = _

  def train(data: DataFrame, maxIter: Int = 10): SVMModel = {
    val training = convertDFtoRDD(data).cache
    svmm = SVMWithSGD.train(training, maxIter)
    svmm
  }

  def getAUC(data: DataFrame): Double ={
    svmm.clearThreshold()
    val rdd = convertDFtoRDD(data)
    val labels = rdd.map(p => p.label)
    val features = rdd.map(p => p.features)
    val scoreAndLabels = labels zip svmm.predict(features)
    // val scoreAndLabels = rdd.map({
    //   p => {
    //     val score = svmm.predict(p.features)
    //     (score, p.label)
    //   }
    // })
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    metrics.areaUnderROC
  }


  def convertDFtoRDD(data: DataFrame): RDD[LabeledPoint] = {
    data.map(row => LabeledPoint(row.getDouble(0), row.getAs[Vector](1)))
  }


  def convertRDDtoLabeledPointRDD(data: RDD[(KeyTuple, ValueTuple)]): RDD[LabeledPoint] = {
    this.sqlContext = new SQLContext(data.context)
    val segmented = data.map({
      case (k,v) => new LabeledPoint(v._2.toDouble, Vectors.dense(v._3.bpDia, v._3.bpSys, v._3.heartRate,
                     v._3.respRate, v._3.temp, v._3.spo2, v._3.gcs, v._3.age.toDouble))
    })
    //this.sqlContext.createDataFrame(segmented).toDF("label", "features")
    segmented
  }
}
