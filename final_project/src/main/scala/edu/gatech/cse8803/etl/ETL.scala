package edu.gatech.cse8803.etl

import edu.gatech.cse8803.model._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ListBuffer}
import scala.collection.mutable
import java.sql.Timestamp
import org.apache.commons.io.FileUtils
import java.io.File
import java.lang.{Double => jDouble}
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg._

object ETL {
  type PatientTuple = (Long, Long, Timestamp, jDouble, jDouble, jDouble,
                       jDouble, jDouble, jDouble, jDouble, jDouble, jDouble,
                       java.lang.Integer)
  type KeyTuple = (Long, Long)
  type ValueTuple = (Timestamp, Int, SummedGCSPatient)
  type FlatPatientTuple = (Timestamp, Int, java.lang.Integer, jDouble, jDouble, jDouble, jDouble,
                             jDouble, jDouble, jDouble)

  def getInsightFeatures(
    patientData: RDD[PatientData], inOut: RDD[InOut],
    septicLabels: RDD[SepticLabel], windowSize: Int
  ): Unit = {

    def getRangeOfK(
      input: List[FlatPatientTuple], index: Int, windowSize: Int
    ): List[FlatPatientTuple] = { input.drop(index).take(windowSize) }

    val windows: RDD[(KeyTuple, List[FlatPatientTuple])] = getWindows(patientData, inOut,
                                                                      septicLabels, windowSize,
                                                                      getRangeOfK)

    //what follows is getting the Mi, Di, Dij, Dijk for each feature
    val emptyPatient = (new java.sql.Timestamp(0), 0, new java.lang.Integer(0), new jDouble(0.0),
                        new jDouble(0.0), new jDouble(0.0), new jDouble(0.0), new jDouble(0.0),
                        new jDouble(0.0), new jDouble(0.0))
    // val means = windows.mapValues({
    //   case (list) => list.reduce(emptyPatient)({
    //     case (acc, entry) => (entry._1, entry._2, entry._3, acc._4 + entry._4,
    //                           acc._5 + entry._5, acc._6 + entry._6, acc._7 + entry._7,
    //                           acc._8 + entry._8, acc._9 + entry._9, acc._10 + entry._10)
    //   })
    // })


  }

  def getSlidingWindowFeatures(
    patientData: RDD[PatientData], inOut: RDD[InOut], septicLabels: RDD[SepticLabel],
    windowSize: Int
  ): DataFrame = {
    val windows: RDD[(KeyTuple, List[FlatPatientTuple])] = getWindows(patientData, inOut,
                                                                      septicLabels, windowSize,
                                                                      getFirstAndKth)
    val slidingWindows: RDD[(Double, Vector)] = windows.mapValues({
      case (v) => (v(0), v(1))
    }).mapValues({
      case(l1, l2) => (l2._2.toDouble, Vectors.dense(l2._3.toDouble, l2._4 - l1._4, l2._5 - l1._5,
                      l2._6 - l1._6, l2._7 - l1._7, l2._8 - l1._8, l2._9 - l1._9,
                      l2._10 - l1._10))
    }).map({
      case (k, v) => v
    })
    val sqlContext = new SQLContext(inOut.context)
    sqlContext.createDataFrame(slidingWindows).toDF("label", "features")
  }

  def getSlidingWindowFeaturesWithOriginalFeatures(
    patientData: RDD[PatientData], inOut: RDD[InOut], septicLabels: RDD[SepticLabel],
    windowSize: Int
  ): RDD[(Double, Vector)] = {

    val windows: RDD[(KeyTuple, List[FlatPatientTuple])] = getWindows(patientData, inOut,
                                                                      septicLabels, windowSize,
                                                                      getFirstAndKth)
    val slidingWindowsWithOrig: RDD[(Double, Vector)] = windows.mapValues({
      case(v) => (v(0), v(1))
    }).mapValues({
      case (l1, l2) => (l2._2.toDouble, Vectors.dense(l2._3.toDouble, l2._4, l2._5, l2._6,
                        l2._7, l2._8, l2._9, l2._10, l2._4 - l1._4, l2._5 - l1._5,
                        l2._6 - l1._6, l2._7 - l1._7, l2._8 - l1._8, l2._9 - l1._9,
                        l2._10 - l1._10))
    }).map({
      case(k,v) => v
    })
    slidingWindowsWithOrig
    // val sqlContext = new SQLContext(inOut.context)
    // sqlContext.createDataFrame(slidingWindowsWithOrig).toDF("label", "features")
  }

  def getWindows(
    patientData: RDD[PatientData], inOut: RDD[InOut], septicLabels: RDD[SepticLabel],
    windowSize: Int,
    innerFunction: (List[FlatPatientTuple], Int, Int) => List[FlatPatientTuple]
  ): RDD[(KeyTuple, List[FlatPatientTuple])] = {
    val dataset: RDD[(KeyTuple, ValueTuple)] = grabFeatures(patientData, inOut, septicLabels)
    val flattened: RDD[(KeyTuple, FlatPatientTuple)] = dataset.mapValues({
      case (timestamp, label, pd) => (
        timestamp, label, pd.age, pd.bpDia, pd.bpSys, pd.heartRate, pd.respRate,
        pd.temp, pd.spo2, pd.gcs
      )
    })
    val grouped = flattened.groupByKey().mapValues(l => l.toList.sortBy(_._1))
    grouped.mapValues({
      case(listOfTuples) => {
        val list = scala.collection.mutable.ListBuffer[List[FlatPatientTuple]]()
        for (i <- 0 to listOfTuples.length - windowSize){
          list += innerFunction(listOfTuples, i, windowSize)
        }
        list.toList
      }
    }).flatMapValues(x => x)
  }


  def grabFeatures(patientData: RDD[PatientData], inOut: RDD[InOut],
                   septicLabels: RDD[SepticLabel]): RDD[(KeyTuple, ValueTuple)] = {

    val emptyTimeSeries = createEmptyTimeSeries(inOut)
    mergeFeatureRDDs(patientData, emptyTimeSeries, septicLabels)
  }

  def grabFeatures(patientData: RDD[PatientData], inOut: RDD[InOut]): RDD[(KeyTuple, ValueTuple)] = {

    val emptyTimeSeries = createEmptyTimeSeries(inOut)
    mergeFeatureRDDs(patientData, emptyTimeSeries)
  }

  def grabFeatures(patientData: RDD[PatientData], inOut: RDD[InOut],
                   septicLabels: RDD[SepticLabel], percentSample: Double): RDD[(KeyTuple, ValueTuple)] = {

    //same as grabFeatures, except it subsamples the patients by percentSample
    //have to guarantee some of the patients are septic
    val sc = patientData.context
    val septicPatients: RDD[(Long, Int)] = septicLabels.map(_.patientId).
                                                distinct.
                                                sample(false, percentSample, 8803).
                                                map( x => (x, 1))
    val septicList: List[Long] = septicLabels.map(_.patientId).distinct.collect.toList
    //find size
    val numSeptic = septicPatients.count
    val nsPatients: RDD[Long] = inOut.map(_.patientId).filter(x => !septicList.contains(x))
    val numNonSeptic = nsPatients.count
    val percentNS: Double = (numNonSeptic * percentSample - numSeptic) / numNonSeptic.toDouble
    val sampledNsPatients: RDD[(Long, Int)] = nsPatients.sample(false, percentNS, 8803).map(x => (x, 0))
    val patientsRdd = sc.union(septicPatients, sampledNsPatients)
    val patients = patientsRdd.map(_._1).collect.toList
    val sampledPatientData = patientData.filter( x => patients.contains(x.patientId)).cache()
    val sampledInOut = inOut.filter(x => patients.contains(x.patientId)).cache()
    val sampledSepticLabels = septicLabels.filter(x => patients.contains(x.patientId)).cache()
    //val file = "file:///home/bdh/project/sampled_subject_ids"
    //patientsRdd.saveAsTextFile(file)
    grabFeatures(sampledPatientData, sampledInOut, sampledSepticLabels)
  }

  def grabFeatures(patientData: RDD[PatientData], inOut: RDD[InOut],
                   percentSample: Double): RDD[(KeyTuple, ValueTuple)] = {

    val sc = patientData.context
    val patientsRdd: RDD[(Long, Int)] = inOut.map(_.patientId).distinct.
                                              sample(false, percentSample, 8803).
                                              map( x => (x, 0))
    val patients = patientsRdd.map(_._1).collect.toList
    val sampledPatientData = patientData.filter( x => patients.contains(x.patientId)).cache()
    val sampledInOut = inOut.filter( x => patients.contains(x.patientId)).cache()
    grabFeatures(sampledPatientData, sampledInOut)
  }

  def mergeFeatureRDDs(patientData: RDD[PatientData],
                       emptyTimeSeries: RDD[((Long, Long, Timestamp), Int)],
                       septicLabels: RDD[SepticLabel]): RDD[(KeyTuple, ValueTuple)] = {
    val sc = patientData.context

    val keyedEvents = patientData.map(
      evt => ((evt.patientId, evt.icuStayId, evt.datetime), evt)
    )
    val keyedLabels = septicLabels.map(
      label => ((label.patientId, label.icuStayId, label.datetime), 1)
    )

    //starts ((patientid, icustayid, datetime), data or label)
    //finishes as ((patientid, icustayid), (datetime, label, data))

    val linkedEvents = emptyTimeSeries.leftOuterJoin(
      keyedEvents
    ).leftOuterJoin(
      keyedLabels
    ).map({
      //k = (patientId, icustayid, datetime)
      //v = ((Int, PatientData), label)
      case (k, v) => ((k._1, k._2), (k._3, v._2, v._1._2))
    })

    val labeledLinkedEvents = linkedEvents.map({
      case (k, v) =>
        ((k._1, k._2), (v._1, v._2.getOrElse(0), v._3.getOrElse(
          new PatientData(k._1, k._2, v._1, null, null, null, null, null, null, null, null, null, null)
        )))
    })
    //this sort by requires the implicit ordering at the bottom of this object
    val groupedEvents = labeledLinkedEvents.groupByKey().mapValues(
      iter => iter.toList.sortBy(_._1)
    )
    //likely don't need combineByKey, can just iterate over the current list
    //since it is ordered.
    //idea: mapValues, create new mutable List, add value one at a time from
    //current list, checking any values that are null against the previous entry
    //will require keeping track of previous entry during iteration, since
    //that is not available after it moves to next
    val forwardImputed = groupedEvents.mapValues({
      case list => {
        val imputedList = new scala.collection.mutable.MutableList[(java.sql.Timestamp, Int, PatientData)]()
        var prevData = list(0)._3
        for (li <- list) {
          val patientData = compareAndForwardImpute(prevData, li._3)
          //yes, this double parens is necessary; first one is for
          //+=, second is to show this is 1 entry, not 3.
          imputedList += ((li._1, li._2, patientData))
          prevData = patientData
        }
        imputedList.toList
      }
    })
    val fullyImputed = forwardImputed.mapValues({
      case list => {
        val imputedList = new scala.collection.mutable.MutableList[(java.sql.Timestamp, Int, PatientData)]()
        var prevData = list(0)._3
        for (li <- list.reverse) {
          val patientData = compareAndForwardImpute(prevData, li._3)
          //yes, this double parens is necessary; first one is for
          //+=, second is to show this is 1 entry, not 3.
          imputedList += ((li._1, li._2, patientData))
          prevData = patientData
        }
        imputedList.toList.reverse
      }
    })
    val flatImputed = fullyImputed.flatMapValues(x => x)

    flatImputed.filter({
      case(k, v) => !patientDataContainsNull(v._3)
    }).mapValues({
      case(t, l, d) => (t, l,
        new SummedGCSPatient(d.patientId, d.icuStayId, d.datetime,
        d.bpDia, d.bpSys, d.heartRate, d.respRate, d.temp, d.spo2,
        d.eyeOp + d.verbal + d.motor, d.age))
    })
  }

  def mergeFeatureRDDs(patientData: RDD[PatientData],
                       emptyTimeSeries: RDD[((Long, Long, Timestamp), Int)]): RDD[(KeyTuple, ValueTuple)] = {
    val sc = patientData.context
    //first turn the gcsEvent into something that can be unioned with chartEvents
    //229000 = (max(itemid) in d_items / 1000 + 1) * 1000
    val keyedEvents: RDD[((Long, Long, Timestamp), PatientData)] = patientData.map(
      evt => ((evt.patientId, evt.icuStayId, evt.datetime), evt)
    )

    val linkedEvents: RDD[((Long, Long), (Timestamp, Int, PatientData))] = emptyTimeSeries.leftOuterJoin(keyedEvents).map({
      case (k,v) => ((k._1, k._2), (k._3, v._1, v._2.getOrElse(
        new PatientData(k._1, k._2, k._3, null, null, null, null, null, null, null, null, null, null)
      )))
    })
    //this sort by requires the implicit ordering at the bottom of this object
    val groupedEvents = linkedEvents.groupByKey().mapValues(
      iter => iter.toList.sortBy(_._1)
    )
    //likely don't need combineByKey, can just iterate over the current list
    //since it is ordered.
    //idea: mapValues, create new mutable List, add value one at a time from
    //current list, checking any values that are null against the previous entry
    //will require keeping track of previous entry during iteration, since
    //that is not available after it moves to next
    val forwardImputed = groupedEvents.mapValues({
      case list => {
        val imputedList = new scala.collection.mutable.MutableList[(java.sql.Timestamp, Int, PatientData)]()
        var prevData = list(0)._3
        for (li <- list) {
          val patientData = compareAndForwardImpute(prevData, li._3)
          //yes, this double parens is necessary; first one is for
          //+=, second is to show this is 1 entry, not 3.
          imputedList += ((li._1, li._2, patientData))
          prevData = patientData
        }
        imputedList.toList
      }
    })
    val fullyImputed = forwardImputed.mapValues({
      case list => {
        val imputedList = new scala.collection.mutable.MutableList[(java.sql.Timestamp, Int, PatientData)]()
        var prevData = list(0)._3
        for (li <- list.reverse) {
          val patientData = compareAndForwardImpute(prevData, li._3)
          //yes, this double parens is necessary; first one is for
          //+=, second is to show this is 1 entry, not 3.
          imputedList += ((li._1, li._2, patientData))
          prevData = patientData
        }
        imputedList.toList.reverse
      }
    })
    val flatImputed = fullyImputed.flatMapValues(x => x)
    flatImputed.filter({
      case(k, v) => !patientDataContainsNull(v._3)
    }).mapValues({
      case(t, l, d) => (t, l,
        new SummedGCSPatient(d.patientId, d.icuStayId, d.datetime,
        d.bpDia, d.bpSys, d.heartRate, d.respRate, d.temp, d.spo2,
        d.eyeOp + d.verbal + d.motor, d.age))
    })
  }

  def createEmptyTimeSeries(inOut: RDD[InOut]): RDD[((Long, Long, Timestamp), Int)] = {
    val intermediate = inOut.map({
      case io => ((io.patientId, io.icustayId), createTimeList(io.intime, io.outtime))
    })
    //turn the v into part of the k, add a 0 (will act as the sepsis label later)
    val expanded = intermediate.flatMapValues(x => x).map({
      case (k,v) => ((k._1, k._2, v), 0)
    })
    expanded
  }

  def getFirstAndKth( input: List[FlatPatientTuple], index: Int, windowSize: Int ): List[FlatPatientTuple] = {
    Seq(input(index), input(index + windowSize - 1)).toList
  }

  def createTimeList(in: Timestamp, out: Timestamp): List[Timestamp] = {
    val timeList: ListBuffer[Timestamp] = new ListBuffer[Timestamp]()
    timeList += in
    var last = in
    while (last.before(out)) {
      last = new Timestamp(last.getTime + (1000 * 60 * 60))
      timeList += last
    }
    timeList.toList
  }

  def compareAndForwardImpute(prevData: PatientData, curData: PatientData): PatientData = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val pList: List[Any] = extractPatientData(prevData)
    val cList: List[Any] = extractPatientData(curData)
    val combinedLists = pList zip cList
    //have to hack around type safety here. Usually awesome, less so here.
    val imputedList:List[String] = combinedLists.map({
      case (prev, cur) => {
        if (cur == null){
          if (prev != null){
            prev.toString
          } else {
            ""
          }
        } else {
          cur.toString
        }
      }
    })
    //let the hacking begin now

    new PatientData(imputedList(0).toLong, imputedList(1).toLong,
                    new Timestamp(dateFormat.parse(imputedList(2)).getTime),
                    checkForNull(imputedList(3)), checkForNull(imputedList(4)),
                    checkForNull(imputedList(5)), checkForNull(imputedList(6)),
                    checkForNull(imputedList(7)), checkForNull(imputedList(8)),
                    checkForNull(imputedList(9)), checkForNull(imputedList(10)),
                    checkForNull(imputedList(11)),
                    if (imputedList(12).length > 0) java.lang.Integer.parseInt(imputedList(12)) else null)

  }

  def checkForNull(value: String): java.lang.Double = {
    if (value.length > 0) java.lang.Double.valueOf(value.toDouble) else null
  }

  def extractPatientData(data: PatientData): List[Any] = {
    Seq(data.patientId, data.icuStayId, data.datetime, data.bpDia, data.bpSys,
        data.heartRate, data.respRate, data.temp, data.spo2, data.eyeOp, data.verbal,
        data.motor, data.age).toList
  }

  def patientDataContainsNull(d: PatientData): Boolean = {
    //patientId and icuStayId are guaranteed to not be null (they're a scala Long)
    d.datetime == null || d.bpDia == null || d.bpSys == null || d.heartRate == null ||
    d.respRate == null || d.temp == null || d.spo2 == null || d.eyeOp == null ||
    d.verbal == null || d.motor == null || d.age == null
  }


  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }
}
