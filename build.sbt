name := "sparktemplate"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-sql" % "1.5.2",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
