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

object Main {
  type SmallMap = mutable.Map[Long, Double]
  type InnerTuple = (Int, SmallMap, SmallMap)
  type MapKeyValue = (Timestamp, InnerTuple)
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)
    /*val dbc = "jdbc:postgresql://hostmachine:9530/postgres?user=postgres&password=Password123"
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(dbc)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    try {
      val prep = conn.prepareStatement("SELECT * FROM admissions WHERE subject_id = 61")
      val rs: ResultSet = prep.executeQuery()
    }*/
    val (chartEvents, gcsEvents, inOut, septicLabels) = loadLocalRddRawData(sqlContext)
    val allItemIds: RDD[Int] = sc.parallelize(Seq(220179, 220050, 228152, 227243, 225167, 220059, 225309,
                    220180, 220051, 228151, 227242, 224643, 220060, 225310,
                    220045, 220210, 223761, 220277, 220739, 223900, 223901,
                    226756, 226758, 228112, 226757, 227011, 227012, 227014, 22900))
    val features: RDD[(Long, MapKeyValue)] = ETL.grabFeatures(chartEvents, gcsEvents,
                                                              inOut, septicLables, allItemIds)
    //need to transform them to be usable by the HmmModel
    val ignore = Seq(227014, 227012, 227011, 226757, 228112, 226758, 226756)
    // TODO: This will need to change to sort by the name associated with the itemid
    //so that carevue and metavision vectors are comparable.
    val preObservations = features.map({
      case (long, (timestamp, (int, mutmap1, mutmap2))) =>
      (long, mutmap1.toList.filter(x => !ignore.contains(x._1)).
                     sortBy(_._1).map(_._2).toArray
    })
    val observationsPerPatient = preObservations.groupByKey.mapValues(_.toList)
    val observations = observationsPerPatient.map({
      case (k, v) => v
    }).collect.toList



    sc.stop()
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
      )//.cache()
    val gcsEvents: RDD[GCSEvent] = sqlContext.sql(
      """
        |SELECT subject_id, charttime, gcs
        |FROM gcs_data
      """.stripMargin).
      map(r => GCSEvent(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime),
                        r(2).toString.toInt))//.cache()

    val inOut: RDD[InOut] = sqlContext.sql(
      """
        |SELECT subject_id, intime, outtime
        |FROM in_out
      """.stripMargin
    ).map( r => InOut(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime),
                      new Timestamp(dateFormat.parse(r(2).toString).getTime)))//.cache()

    val septicLabels: RDD[SepticLabel] = sqlContext.sql(
      """
        |SELECT *
        |FROM septic_id_timestamp
      """.stripMargin
    ).map( r => SepticLabel(r(0).toString.toLong, new Timestamp(dateFormat.parse(r(1).toString).getTime)))//.cache()
    (chartEvents, gcsEvents, inOut, septicLabels)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
