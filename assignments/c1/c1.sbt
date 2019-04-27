name := "C1 Assignment"

version := "1.0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.1"
)