name := "scala-spark-app"

version := "1.0"

scalaVersion := "2.11.8"

lazy val sparkVersion = "2.0.2"

libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core" % sparkVersion,
                             "org.apache.spark" %% "spark-mllib" % sparkVersion)