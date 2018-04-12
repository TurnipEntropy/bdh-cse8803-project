/**
 *
 */

package edu.gatech.cse8803.main

import java.sql.{DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
import org.postgresql._
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.etl.ETL
import scala.collection.mutable
import scala.collection.JavaConverters._
import be.ac.ulg.montefiore.run.jahmm._
import be.ac.ulg.montefiore.run.jahmm.learn._
import serialize.MyRegistrator

object Main {
  type SmallMap = mutable.Map[Long, Double]
  type InnerTuple = (Int, SmallMap, SmallMap)
  type MapKeyValue = (Timestamp, InnerTuple)
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = Main.createContext
    val sqlContext = new SQLContext(sc)
    /*val dbc = "jdbc:postgresql://hostmachine:9530/postgres?user=postgres&password=Password123"
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(dbc)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    try {
      val prep = conn.prepareStatement("SELECT * FROM admissions WHERE subject_id = 61")
      val rs: ResultSet = prep.executeQuery()
    }*/

    // val (cvChartEvents, cvGcsEvents, cvInOut) = Main.loadLocalRddRawDataCareVue(sqlContext)
    // val cvAllItemIds: RDD[Long] = sc.parallelize(Seq(211, 618, 646, 678, 442, 51, 8368, 229000))
    // val features:RDD[(Long, MapKeyValue)] = ETL.grabFeatures(cvChartEvents, cvGcsEvents,
    //                                                           cvInOut, cvAllItemIds, 0.2).cache()
    // features.take(1)
    // //SUPER HACKY, but was kind of fun...
    // val cvPreObservations: RDD[(Long, Array[Double])] = features.map({
    //   case(long, (timestamp, (int, mutmap1, mutmap2))) =>
    //     (long, mutmap1.toList.sortWith({
    //       (a,b) => if (a._1 ==  211) { true } else { if (b._1 ==  211) { false } else {
    //         if (a._1 ==  618) { true } else {if (b._1 ==  618) { false } else {
    //           if (a._1 ==  646) { true } else {if (b._1 ==  646) { false } else {
    //             if (a._1 ==  678) { true } else {if (b._1 ==  678) { false } else {
    //               if (a._1 ==  442) { true } else {if (b._1 ==  442) { false } else {
    //                 if (a._1 ==  51) { true } else {if (b._1 ==  51) { false } else {
    //                   b._1 - a._1 > 0
    //                 }}
    //               }}
    //             }}
    //           }}
    //         }}
    //       }}
    //     }).map(_._2).toArray)
    //   }).cache()
    //
    // val cvObservationsPerPatient = cvPreObservations.groupByKey
    /*
    val (chartEvents, gcsEvents, inOut, septicLabels) = Main.loadLocalRddRawData(sqlContext)
    val allItemIds: RDD[Long] = sc.parallelize(Seq(220179, 220050, 228152, 227243, 224167, 220059, 225309,
                    220180, 220051, 228151, 227242, 224643, 220060, 225310,
                    220045, 220210, 223761, 220277, 220739, 223900, 223901,
                    226756, 226758, 228112, 226757, 227011, 227012, 227014, 229000))
    val features: RDD[(Long, MapKeyValue)] = ETL.grabFeatures(chartEvents, gcsEvents,
                                                              inOut, septicLabels, allItemIds, 0.2).cache()
    features.take(1)
    //need to transform them to be usable by the HmmModel
    val ignore = Seq(227014, 227012, 227011, 226757, 228112, 226758, 226756, 220179, 220050,228152, 227243, 224167, 220059,
                     220180, 220051, 228151, 227242, 224643, 220060, 220739, 223900, 223901)
    // TODO: Was going to make alphabetical, instead just doing a map for carevue
    val preObservations: RDD[(Long, Array[Double])] = features.map({
      case (long, (timestamp, (int, mutmap1, mutmap2))) =>
        (long, mutmap1.toList.filter(x => !ignore.contains(x._1)).
        sortBy(_._1).map(_._2).toArray)
    }).cache()
    val observationsPerPatient = preObservations.groupByKey.mapValues(_.toList.asJava)
    val observations = observationsPerPatient.map({
      case (k, v) => v
    }).collect.toList.asJava

    var i = 0
    val obsList = new java.util.ArrayList[java.util.ArrayList[ObservationVector]] ()
    while (i < observations.size()) {
      var j = 0
      obsList.add(new java.util.ArrayList[ObservationVector])
      while (j < observations.get(i).size()) {
        if (observations.get(i).get(j).length == 9) {
          obsList.get(i).add(new ObservationVector(observations.get(i).get(j)))
        }
        j += 1
      }
      i += 1
    }

    val kml: KMeansLearner[ObservationVector] = new KMeansLearner[ObservationVector] (
      2, new OpdfMultiGaussianFactory(9), obsList
    )

    //Will need to get this working after serialization; workaround is above
    /*
    val preObservations: RDD[(Long, ObservationVector)] = features.map({
      case (long, (timestamp, (int, mutmap1, mutmap2))) =>
      (long, new ObservationVector(mutmap1.toList.filter(x => !ignore.contains(x._1)).
                     sortBy(_._1).map(_._2).toArray))
    })
    val observationsPerPatient = preObservations.groupByKey.mapValues(_.toList.asJava)
    val observations = observationsPerPatient.map({
      case (k, v) => v
    }).collect.toList.asJava*/
    //now have a List<List<ObservationVector>>, can just run HMM I think
   */

    sc.stop()
  }

  def loadLocalRddMetavisionData(sqlContext: SQLContext): (RDD[PatientData], RDD[InOut], RDD[SepticLabel]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val homeString = "file:///home/bdh/project/"
    List(
      homeString + "metavision_all_features.csv",
      homeString + "in_out.csv",
      homeString + "septic_id_timestamp.csv"
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
      filter({ case pdata => pdata.datetime != null}).cache
    patientData.take(1)
    val inOut: RDD[InOut] = sqlContext.sql(
        """
          |SELECT subject_id, icustay_id, intime, outtime
          |FROM in_out
        """.stripMargin
    ).map( r => InOut(r(0).toString.toLong, r(1).toString.toLong,
                        checkDate(r(0).toString, r(2).toString),
                        checkDate(r(0).toString, r(3).toString))).cache()
    inOut.take(1)

    val septicLabels: RDD[SepticLabel] = sqlContext.sql(
        """
          |SELECT *
          |FROM septic_id_timestamp
        """.stripMargin
    ).map( r => SepticLabel(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime))).cache()
    septicLabels.take(1)

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
      ).cache()
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
        |SELECT subject_id, intime, outtime
        |FROM carevue_in_out
      """.stripMargin
    ).
    map( r => (r(0), r(1), r(2))).
    filter (r => r._3.toString.length > 0).
    map( r => InOut(r._1.toString.toLong, new Timestamp(dateFormat.parse(r._2.toString).getTime),
                      new Timestamp(dateFormat.parse(r._3.toString).getTime))).cache()
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
        |SELECT subject_id, intime, outtime
        |FROM in_out
      """.stripMargin
    ).map( r => InOut(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime),
                      new Timestamp(dateFormat.parse(r(2).toString).getTime))).cache()
    inOut.take(1)

    val septicLabels: RDD[SepticLabel] = sqlContext.sql(
      """
        |SELECT *
        |FROM septic_id_timestamp
      """.stripMargin
    ).map( r => SepticLabel(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime))).cache()
    septicLabels.take(1)

    (chartEvents, gcsEvents, inOut, septicLabels)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    conf.set("spark.kryo.registrator", "serialize.MyRegistrator")
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
      println(strSID)
      null
    } else {
      new Timestamp(dateFormat.parse(strDate).getTime)
    }
  }

  def runTest(): Unit = {
    val sc = createContext
    val sqlContext = new SQLContext(sc)
    val (patientData, inOut, septicLabels) = loadLocalRddMetavisionData(sqlContext)
  }
}
