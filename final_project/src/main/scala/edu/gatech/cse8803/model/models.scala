/**
 *
 */

package edu.gatech.cse8803.model

import java.sql.Timestamp


case class ChartEvent(patientId: Long, datetime: Timestamp, itemid: Long, value: Double)

case class GCSEvent(patientId: Long, datetime: Timestamp, gcsScore: Int)

case class InOut(patientId: Long, intime: Timestamp, outtime: Timestamp)

case class SepticLabel(patientId: Long, datetime: Timestamp)

abstract class VertexProperty

case class PatientProperty(patientID: String, sex: String, dob: String, dod: String) extends VertexProperty

case class LabResultProperty(testName: String) extends VertexProperty

case class DiagnosticProperty(icd9code: String) extends VertexProperty

case class MedicationProperty(medicine: String) extends VertexProperty

abstract class EdgeProperty

case class SampleEdgeProperty(name: String = "Sample") extends EdgeProperty


