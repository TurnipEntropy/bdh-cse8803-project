import AssemblyKeys._
seq(assemblySettings: _*)

name := "final_project"

version := "1.0"

scalaVersion := "2.10.5"

retrieveManaged := true
resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("public")
)

evictionWarningOptions in update :=
  EvictionWarningOptions.default
    .withWarnTransitiveEvictions(false)
    .withWarnDirectEvictions(false)
    .withWarnScalaVersionEviction(false)


libraryDependencies ++= Seq(
  "org.apache.spark"          % "spark-core_2.10"              % "1.6.1",
  "org.apache.spark"          % "spark-mllib_2.10"             % "1.6.1",
  "org.apache.spark"          % "spark-graphx_2.10"            % "1.6.1",
  "com.databricks"            % "spark-csv_2.10"               % "1.4.0",
  "org.postgresql"            % "postgresql"                   % "9.3-1102-jdbc41",
  "com.esotericsoftware.kryo" % "kryo"                         % "2.10"
)

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

parallelExecution in Test := false

mainClass in assembly := Some("Main")
