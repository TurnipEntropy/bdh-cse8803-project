/**
 *
 */

package edu.gatech.cse8803.model

import java.sql.Timestamp
import java.lang.{Double => jDouble}

case class ChartEvent(patientId: Long, datetime: Timestamp, itemid: Long, value: Double)

case class GCSEvent(patientId: Long, datetime: Timestamp, gcsScore: Int)

case class InOut(patientId: Long, icustayId: Long, intime: Timestamp, outtime: Timestamp)

case class SepticLabel(patientId: Long, icuStayId: Long, datetime: Timestamp)

case class PatientData(patientId: Long, icuStayId: Long, datetime: Timestamp,
                       bpDia: jDouble, bpSys: jDouble, heartRate: jDouble,
                       respRate: jDouble, temp: jDouble, spo2: jDouble,
                       eyeOp: jDouble, verbal: jDouble, motor: jDouble,
                       age: java.lang.Integer)

case class SummedGCSPatient(patientId: Long, icuStayId: Long, datetime: Timestamp,
                       bpDia: jDouble, bpSys: jDouble, heartRate: jDouble,
                       respRate: jDouble, temp: jDouble, spo2: jDouble,
                       gcs: jDouble, age: java.lang.Integer)
abstract class VertexProperty

case class PatientProperty(patientID: String, sex: String, dob: String, dod: String) extends VertexProperty

case class LabResultProperty(testName: String) extends VertexProperty

case class DiagnosticProperty(icd9code: String) extends VertexProperty

case class MedicationProperty(medicine: String) extends VertexProperty

abstract class EdgeProperty

case class SampleEdgeProperty(name: String = "Sample") extends EdgeProperty
