name := "exploring-data"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core" % "1.2.0",
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-viz" % "0.12"
)
