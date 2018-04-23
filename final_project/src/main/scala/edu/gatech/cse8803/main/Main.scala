/**
 *
 */

package edu.gatech.cse8803.main

import java.sql.{DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.lang.{Double => jDouble}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
import org.postgresql._
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.etl.ETL
import scala.collection.mutable
import org.apache.spark.sql.DataFrame
import edu.gatech.cse8803.ml_models._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.functions.{udf, col, monotonicallyIncreasingId}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.Row

object Main {
  type PatientTuple = (Long, Long, Timestamp, jDouble, jDouble, jDouble,
                       jDouble, jDouble, jDouble, jDouble, jDouble, jDouble,
                       java.lang.Integer)
  type KeyTuple = (Long, Long)
  type ValueTuple = (Timestamp, Int, SummedGCSPatient)
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = Main.createContext
    val sqlContext = new SQLContext(sc)
    sqlContext.sql("set spark.sql.shuffle.partitions=3")
    import sqlContext.implicits._

    // val slidingDataset: DataFrame = ETL.getSlidingWindowFeatures(
    //   metaPatients, metaInOut, metaSepticLabels, 5
    // ).cache
    //slidingOrigDataset.count
    //slidingDataset.count
    if (args(0) == "no-yarn") {
      val (metaPatients, metaInOut, metaSepticLabels) = loadLocalRddMetavisionData(sqlContext)
      println("Performing ETL")
      val slidingOrigDataset: RDD[(Double, Vector)] = ETL.getSlidingWindowFeaturesWithOriginalFeatures(
        metaPatients, metaInOut, metaSepticLabels, 3
      ).cache
      slidingOrigDataset.take(125)
      val negativeSet = slidingOrigDataset.filter(x => x._1 == 0.0).zipWithIndex.cache
      val positiveSet = slidingOrigDataset.filter(x => x._1 == 1.0).zipWithIndex.cache
      slidingOrigDataset.unpersist()
      val negativeSetSize = negativeSet.count
      val positiveSetSize = positiveSet.count
      val negativeSetTrainingSize = negativeSetSize * 4 / 5
      val positiveSetTrainingSize = positiveSetSize * 4 / 5
      val negativeSetTestingSize = negativeSetSize - negativeSetTrainingSize
      val positiveSetTestingSize = positiveSetSize - positiveSetTrainingSize
      var balancingRatio = 0.97
      var mixing = 0.15
      val calculateWeights = udf { d: Double =>
        if (d == 1.0) {
          balancingRatio
        } else {
          1.0 - balancingRatio
        }
      }
      for (i <- 0 to 4) {
        val negTraining = negativeSet.filter(
          x => x._2 < i * negativeSetTestingSize || x._2 >= (i + 1) * negativeSetTestingSize
        ).map({
          x => x._1
        })

        val posTraining = positiveSet.filter(
          x => x._2 < i * positiveSetTestingSize || x._2 >= (i + 1) * positiveSetTrainingSize
        ).map({
          x => x._1
        })

        val negTesting = negativeSet.filter(
          x => x._2 < (i + 1) * negativeSetTestingSize && x._2 >= i * negativeSetTestingSize
        ).map({
          x => x._1
        })

        val posTesting = positiveSet.filter(
          x => x._2 < (i + 1) * positiveSetTestingSize && x._2 >= i * positiveSetTestingSize
        ).map({
          x => x._1
        })

        val training: DataFrame = sqlContext.createDataFrame(negTraining.union(posTraining)).toDF("label", "features").cache
        val testing: DataFrame = sqlContext.createDataFrame(negTesting.union(posTraining)).toDF("label", "features").cache
        //treat first set of training data as hyper-parameter tuning step
        if (i == 0) {
          var bestRatio: Double = 0.0
          var bestMixing: Double = 0.0
          var bestAUC: Double = 0.0
          for (preRatio <- 95 to 99) {
            balancingRatio = preRatio.toDouble / 100.0
            val weightedTraining = training.withColumn("classWeightCol", calculateWeights(training("label")))
            for (preMixing <- 2 to 8) {
              val elasticMixing = preMixing.toDouble / 10.0
              val enlc = new ElasticNetLogClassifier(standardize = true, maxIter = 10, addWeightsCol = true, elasticNetParam = elasticMixing)
              val enlcm = enlc.train(weightedTraining)
              val auc = enlc.getAUC()
              if (auc > bestAUC) {
                bestAUC = auc
                bestRatio = balancingRatio
                bestMixing = elasticMixing
              }
            }
          }
          balancingRatio = bestRatio
          mixing = bestMixing
        } else {

          val weightedTraining = training.withColumn("classWeightCol", calculateWeights(training("label")))
          val enlc = new ElasticNetLogClassifier(standardize = true, maxIter = 20, addWeightsCol = true, elasticNetParam = mixing)
          val enlcm = enlc.train(weightedTraining)
          val predictions = enlc.predict(testing)
          predictions.write.format("com.databricks.spark.csv").save("file:///home/bdh/project/predictions/" + i.toString)
          enlc.saveModel("file:///home/bdh/project/logistic_classifier_" + i)
          training.unpersist()
          testing.unpersist()
        }
      }

    }

    if (args(0) == "predict-carevue") {
      val threshold = args(1).toDouble
      val (carePatients, careInOut) = loadLocalRddCareVueData(sqlContext)
      val slidingOrigDataset: RDD[((Long, Long, Timestamp), (Double, Vector))] =
        ETL.getSlidingWindowFeaturesWithOriginalFeaturesCareVue(carePatients, careInOut, 5)
      val withoutKey:RDD[(Double, Vector)] = slidingOrigDataset.values
      val carevueData = sqlContext.createDataFrame(withoutKey).toDF("label", "features")
      val enlcm = LogisticRegressionModel.read.load("file:///home/bdh/project/logistic_classifiers/logistic_classifier_1").setThreshold(threshold)
      val preds = enlcm.transform(carevueData)
      val outputPreds = preds.select("prediction", "features").withColumn("id", monotonicallyIncreasingId).rdd.map({
        r => (r(2).toString.toLong, (r(0).toString, r(1).toString))
      })
      val output = slidingOrigDataset.zipWithIndex.map({
        case (k,v) => (v, k._1)
      }).join(outputPreds)
      output.saveAsTextFile("file:///home/bdh/project/predicted_carevue")

    }
    if (args(0) == "etl") {
      println("Running ETL and saving out CSVs of training and test data")
      println("Loading Data")
      val (metaPatients, metaInOut, metaSepticLabels) = loadLocalRddMetavisionData(sqlContext)
      println("Performing ETL")
      val slidingOrigDataset: RDD[(Double, Vector)] = ETL.getSlidingWindowFeaturesWithOriginalFeatures(
        metaPatients, metaInOut, metaSepticLabels, 5
      )
      metaPatients.unpersist()
      metaInOut.unpersist()
      metaSepticLabels.unpersist()
      slidingOrigDataset.take(125)
      //val negativeArray = slidingOrigDataset.filter($"label" === "0").randomSplit(Array(0.75, 0.25), 8803L)
      //val positiveArray = slidingOrigDataset.filter($"label" === "1").randomSplit(Array(0.75, 0.25), 8803L)
      val negativeArray = slidingOrigDataset.filter(x => x._1 == 0.0).randomSplit(Array(0.75, 0.25), 6630L)
      val positiveArray = slidingOrigDataset.filter(x => x._1 == 1.0).randomSplit(Array(0.75, 0.25), 6630L)

      //negativeArray.take(125000)
      //positiveArray.take(125000)
      negativeArray(0).union(positiveArray(0)).saveAsTextFile("file:///home/bdh/project/training_data")
      negativeArray(1).union(positiveArray(1)).saveAsTextFile("file:///home/bdh/project/test_data")
      // negativeArray(0).unionAll(positiveArray(0)).coalesce(1).write.format("com.databricks.spark.csv").save("file:///home/bdh/project/training_data")
      // negativeArray(1).unionAll(positiveArray(1)).coalesce(1).write.format("com.databricks.spark.csv").save("file:///home/bdh/project/test_data")

    } else if (args(0) == "logistic-training") {
      val trainingData: DataFrame = loadTrainingDataFrame(sqlContext)
      val enlc = new ElasticNetLogClassifier(standardize = true)
      val enlcm = enlc.train(trainingData)
      enlcm.write.save("file:///home/bdh/logistic_classifier")
      println(enlc.getAUC())


    } else if (args(0) == "predict-carevue-old") {
      val (carePatients, careInOut) = loadLocalRddCareVueData(sqlContext)
      val slidingOrigDataset: RDD[((Long, Long, Timestamp), (Double, Vector))] =
        ETL.getSlidingWindowFeaturesWithOriginalFeaturesCareVue(carePatients, careInOut, 5)
      val withoutKey: RDD[(Double, Vector)] = slidingOrigDataset.values
      val carevueData = sqlContext.createDataFrame(withoutKey).toDF("label", "features")
      val enlcm = LogisticRegressionModel.read.load("file:///home/bdh/logistic_classifier")
      val preds = enlcm.transform(carevueData)
      val outputPreds = preds.select("prediction").withColumn("id", monotonicallyIncreasingId).rdd.map({
        r => (r(1).toString.toLong, r(0).toString)
      })
      val output = slidingOrigDataset.zipWithIndex.map({
        case (k, v) => (v, k._1)
      }).join(outputPreds)
      output.saveAsTextFile("file:///home/bdh/predicted_carevue")
    }
    // else if (args(0) == "logistic") {
    //   println("Running Logistic Classifier")
    //   val enlc = new ElasticNetLogClassifier(standardize = true)
    //   val enlcm = enlc.train(slidingOrigDataset)
    //   val enlcAUC = enlc.getAUC()
    //   println(s"Logistic Classifier AUC: $enlcAUC")
    //   enlc.getEvaluationMetrics(slidingOrigDataset).collect().foreach(println)
    // } else if (args(0) == "randomforest") {
    //   println("Running Random Forest")
    //   val rf = new RandomForest()
    //   val dataArray = slidingOrigDataset.filter($"label" === "0").randomSplit(Array(0.75, 0.25), 8803L)
    //   val training = dataArray(0)
    //   val testing = dataArray(1)
    //   val rfm = rf.train(training, 8)
    //   val rfAUC = rf.getAUC(testing)
    //   slidingOrigDataset.unpersist
    //   println(s"Random Forest AUC: $rfAUC")
    //   rf.getEvaluationMetrics().collect().foreach(println)
    // } else {
    //   println("Running SVM")
    //   val svm = new SVM()
    //   val svmm = svm.train(slidingOrigDataset)
    //   val svmmAUC = svm.getAUC(slidingOrigDataset)
    //   println(s"SVM AUC: $svmmAUC")
    // }
    //println("Logistic Classifier AUC: $enlcAUC\nRandom Forest AUC: $rfAUC\nSVM AUC: $svmmAUC")

    //val file = "file:///home/bdh/project/newly_labeled_dataset"
    //dataset.saveAsTextFile(file)
    //val file = "file:///home/bdh/project/sampled_subject_ids"
    //patientsRdd.saveAsTextFile(file)
    //val (cvPatients, cvInout) = loadLocalRddRawDataCareVue(sqlContext)

    sc.stop()
  }

  def loadLocalRddCareVueData(sqlContext: SQLContext): (RDD[SummedGCSPatient], RDD[InOut]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val homeString = "file:///home/bdh/project/"
    List(
      homeString + "carevue_all_features.csv",
      homeString + "in_out_cv.csv"
    ).foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patientData: RDD[SummedGCSPatient] = sqlContext.sql(
      """
         |SELECT subject_id, icustay_id, charttime, bp_dia, bp_sys, heart_rate,
         |       resp_rate, temp_f, spo2, gcs, age
         |FROM carevue_all_features
      """.stripMargin).
      map(r => SummedGCSPatient(r(0).toString.toLong, r(1).toString.toLong,
                             checkDate(r(0).toString, r(2).toString),
                             checkForNull(r(3)), checkForNull(r(4)),
                             checkForNull(r(5)), checkForNull(r(6)),
                             checkForNull(r(7)), checkForNull(r(8)),
                             checkForNull(r(9)),
              if (r(10).toString.length > 0) java.lang.Integer.valueOf(r(10).toString.toInt) else null)
      ).
      filter({ case pdata => pdata.datetime != null})

    val inOut: RDD[InOut] = sqlContext.sql(
        """
          |SELECT subject_id, icustay_id, intime, outtime
          |FROM in_out_cv
        """.stripMargin
    ).filter("intime is not null and outtime is not null and intime != '' and outtime != ''").
      map( r => InOut(r(0).toString.toLong, r(1).toString.toLong,
                        checkDate(r(0).toString, r(2).toString),
                        checkDate(r(0).toString, r(3).toString))).cache()

    (patientData, inOut)
  }
  def loadTrainingDataFrame(sqlContext: SQLContext): DataFrame = {
    val homeString = "file:///home/bdh/project/training/"
    val dfList = List(
      homeString + "part-00000",
      homeString + "part-00001",
      homeString + "part-00002",
      homeString + "part-00003",
      homeString + "part-00004",
      homeString + "part-00005",
      homeString + "part-00006",
      homeString + "part-00007"
    ).map(sqlContext.read.format("com.databricks.spark.csv").options(Map("header" -> "false")).load(_).toDF)

    val oneDF = dfList.reduce{ (acc, entry) => acc.unionAll(entry) }
    def removeSpecialCharacters: String => Double = _.replaceAll("[^a-zA-Z0-9.]", "").toDouble
    val specialCharacterUDF = udf(removeSpecialCharacters)
    val cleansedDF = oneDF.select(oneDF.columns.map(c => specialCharacterUDF(col(c)).alias(c)): _*)
    //now that the annoying as all get out part is over... time for more annoying parts

    val assembler = new VectorAssembler().setInputCols(Array("C1", "C2", "C3", "C4",
                                                             "C5", "C6", "C7", "C8",
                                                             "C9", "C10", "C11",
                                                             "C12", "C13", "C14", "C15")
                                                      ).setOutputCol("features")
    assembler.transform(cleansedDF).select("C0", "features").withColumnRenamed("C0", "label")
  }

  def loadTestDataFrame(sqlContext: SQLContext): DataFrame = {
    val homeString = "file:///home/bdh/project/test_data/"
    val dfList = List(
      homeString + "part-00000",
      homeString + "part-00001",
      homeString + "part-00002",
      homeString + "part-00003",
      homeString + "part-00004",
      homeString + "part-00005",
      homeString + "part-00006",
      homeString + "part-00007"
    ).map(sqlContext.read.format("com.databricks.spark.csv").options(Map("header" -> "false")).load(_).toDF)

    val oneDF = dfList.reduce{ (acc, entry) => acc.unionAll(entry) }
    def removeSpecialCharacters: String => Double = _.replaceAll("[^a-zA-Z0-9.]", "").toDouble
    val specialCharacterUDF = udf(removeSpecialCharacters)
    val cleansedDF = oneDF.select(oneDF.columns.map(c => specialCharacterUDF(col(c)).alias(c)): _*)
    //now that the annoying as all get out part is over... time for more annoying parts

    val assembler = new VectorAssembler().setInputCols(Array("C1", "C2", "C3", "C4",
                                                             "C5", "C6", "C7", "C8",
                                                             "C9", "C10", "C11",
                                                             "C12", "C13", "C14", "C15")
                                                      ).setOutputCol("features")
    assembler.transform(cleansedDF).select("C0", "features").withColumnRenamed("C0", "label")
  }
  def loadLocalRddMetavisionData(sqlContext: SQLContext): (RDD[PatientData], RDD[InOut], RDD[SepticLabel]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val homeString = "file:///home/bdh/project/"
    List(
      homeString + "metavision_all_features.csv",
      homeString + "in_out.csv",
      homeString + "septic_label_icustay_2.csv"
    ).foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patientData: RDD[PatientData] = sqlContext.sql(
      """
         |SELECT subject_id, icustay_id, charttime, bp_dia, bp_sys, heart_rate,
         |       resp_rate, temp_f, spo2, eye_opening, verbal, motor, age
         |FROM metavision_all_features
      """.stripMargin).
      map(r => PatientData(r(0).toString.toLong, r(1).toString.toLong,
                             checkDate(r(0).toString, r(2).toString),
                             checkForNull(r(3)), checkForNull(r(4)),
                             checkForNull(r(5)), checkForNull(r(6)),
                             checkForNull(r(7)), checkForNull(r(8)),
                             checkForNull(r(9)), checkForNull(r(10)),
                             checkForNull(r(11)),
              if (r(12).toString.length > 0) java.lang.Integer.valueOf(r(12).toString.toInt) else null)
      ).
      filter({ case pdata => pdata.datetime != null})

    val inOut: RDD[InOut] = sqlContext.sql(
        """
          |SELECT subject_id, icustay_id, intime, outtime
          |FROM in_out
        """.stripMargin
    ).map( r => InOut(r(0).toString.toLong, r(1).toString.toLong,
                        checkDate(r(0).toString, r(2).toString),
                        checkDate(r(0).toString, r(3).toString))).cache()


    val septicLabels: RDD[SepticLabel] = sqlContext.sql(
        """
          |SELECT *
          |FROM septic_label_icustay_2
        """.stripMargin
    ).map( r => SepticLabel(r(0).toString.toLong, r(1).toString.toLong, new Timestamp(dateFormat.parse(r(2).toString).getTime)))


    (patientData, inOut, septicLabels)


  }
  def loadLocalRddRawDataCareVue(sqlContext: SQLContext): (RDD[ChartEvent], RDD[GCSEvent], RDD[InOut]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val homeString = "file:///home/bdh/project/"
    List(
      homeString + "carevue_gcs_data.csv",
      homeString + "carevue_in_out.csv",
      homeString + "carevue_data.csv"
    ).foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val chartEvents: RDD[ChartEvent] = sqlContext.sql(
      """
        |SELECT subject_id, charttime, itemid, avg_value
        |FROM carevue_data
      """.stripMargin).
      map(r => ChartEvent(r(0).toString.toLong,
                          new Timestamp(dateFormat.parse(r(1).toString).getTime),
                          r(2).toString.toLong,
                          r(3).toString.toDouble
                          )
      )
      chartEvents.take(1)

    val gcsEvents: RDD[GCSEvent] = sqlContext.sql(
      """
        |SELECT subject_id, charttime, gcs
        |FROM carevue_gcs_data
      """.stripMargin).
      map(r => GCSEvent(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime),
                        r(2).toString.toInt)).cache()
    gcsEvents.take(1)

    val inOut: RDD[InOut] = sqlContext.sql(
      """
        |SELECT subject_id, icustay_id, intime, outtime
        |FROM carevue_in_out
      """.stripMargin
    ).
    map( r => (r(0), r(1), r(2), r(3))).
    filter (r => r._3.toString.length > 0).
    map( r => InOut(r._1.toString.toLong, r._2.toString.toLong,
                      new Timestamp(dateFormat.parse(r._3.toString).getTime),
                      new Timestamp(dateFormat.parse(r._4.toString).getTime))).cache()
    inOut.take(1)
    val sc = chartEvents.context
    val cePatients = chartEvents.map(_.patientId).distinct.map(x => (x, 1))
    val gPatients = gcsEvents.map(_.patientId).distinct.map(x => (x, 1))
    val ioPatients = inOut.map(_.patientId).distinct.map(x => (x, 1))
    val patients = cePatients.join(gPatients).join(ioPatients).map(_._1).collect

    val fchartEvents = chartEvents.filter(x => patients.contains(x.patientId)).cache()
    val fgcsEvents = gcsEvents.filter(x => patients.contains(x.patientId)).cache()
    val finOut = inOut.filter(x => patients.contains(x.patientId)).cache()


    (fchartEvents, fgcsEvents, finOut)
  }

  def loadLocalRddRawData(sqlContext: SQLContext): (RDD[ChartEvent], RDD[GCSEvent], RDD[InOut], RDD[SepticLabel]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val homeString = "file:///home/bdh/project/"
    List(
      homeString + "gcs_data.csv",
      homeString + "in_out.csv",
      homeString + "metavision_data.csv",
      homeString + "septic_id_timestamp.csv"
    ).foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val chartEvents: RDD[ChartEvent] = sqlContext.sql(
      """
        |SELECT subject_id, charttime, itemid, avg_value
        |FROM metavision_data
      """.stripMargin).
      map(r => ChartEvent(r(0).toString.toLong,
                          new Timestamp(dateFormat.parse(r(1).toString).getTime),
                          r(2).toString.toLong,
                          r(3).toString.toDouble
                          )
      ).cache()
      chartEvents.take(1)

    val gcsEvents: RDD[GCSEvent] = sqlContext.sql(
      """
        |SELECT subject_id, charttime, gcs
        |FROM gcs_data
      """.stripMargin).
      map(r => GCSEvent(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime),
                        r(2).toString.toInt)).cache()
    gcsEvents.take(1)

    val inOut: RDD[InOut] = sqlContext.sql(
      """
        |SELECT subject_id, icustay_id, intime, outtime
        |FROM in_out
      """.stripMargin
    ).map( r => InOut(r(0).toString.toLong, r(1).toString.toLong,
                      new Timestamp(dateFormat.parse(r(2).toString).getTime),
                      new Timestamp(dateFormat.parse(r(3).toString).getTime))).cache()
    inOut.take(1)

    val septicLabels: RDD[SepticLabel] = sqlContext.sql(
      """
        |SELECT *
        |FROM septic_id_timestamp
      """.stripMargin
    ).map( r => SepticLabel(r(0).toString.toLong, r(1).toString.toLong, new Timestamp(dateFormat.parse(r(2).toString).getTime))).cache()
    septicLabels.take(1)

    (chartEvents, gcsEvents, inOut, septicLabels)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")

  def loadPythonCSV(csv: RDD[(String, String)]): RDD[((Long, Int), (Double, Double))] = {
    val mapCSV: RDD[(Array[String], Array[String])] = csv.map({
      case (s1, s2) => (s1.replace("(", "").replace(")", "").replace("'", "").split(","),
                        s2.replace("[", "").replace("]", "").split(","))
    })
    mapCSV.map({
      case (arr1, arr2) => ((arr1(1).trim.toLong, arr1(0).trim.toInt),
                            (arr2(0).trim.toDouble, arr2(1).trim.toDouble))
    })
  }

  def checkForNull(rowVal: Any): java.lang.Double = {
    if (rowVal.toString.length > 0) java.lang.Double.valueOf(rowVal.toString.toDouble) else null
  }

  def checkDate(strSID: String, strDate: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (strDate.length <= 0) {
      //println(strSID)
      null
    } else {
      new Timestamp(dateFormat.parse(strDate).getTime)
    }
  }

  // def quickerStart(): DataFrame = {
  //   val sc = createContext
  //   val sqlContext = new SQLContext(sc)
  //   val (patientData, inOut, septicLabels) = loadLocalRddMetavisionData(sqlContext)
  //   ETL.getSlidingWindowFeaturesWithOriginalFeatures(patientData, inOut, septicLabels, 5)
  // }

  def quickStart(): RDD[(KeyTuple, ValueTuple)] = {
    val sc = createContext
    val sqlContext = new SQLContext(sc)
    val (patientData, inOut, septicLabels) = loadLocalRddMetavisionData(sqlContext)
    ETL.grabFeatures(patientData, inOut, septicLabels)
  }
}
