name := "hmm_project"

version := "0.1"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.apache.spark"  % "spark-core_2.10"              % "1.3.1" % "provided",
  "org.apache.spark"  % "spark-mllib_2.10"             % "1.3.1",
  "org.apache.spark"  % "spark-graphx_2.10"            % "1.3.1",
  "com.databricks"    % "spark-csv_2.10"               % "1.3.0"
)