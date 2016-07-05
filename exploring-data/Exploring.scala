package exploring

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import breeze.linalg._
import breeze.plot._

/**
  * To run this just use `sbt run` - there is a bug in sbt (I think) when you use `run` where it doesn't shut something
  *  down and it kills your pc. I'm using 3rd party deps here for the charts, so sending it to spark is a pain
  *  because it needs those jars (it's doable... but it's easier to do sbt run...)
  */
object Exploring {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "exploring-data")

    val userData = sc.textFile("../data/ml-100k/u.user")

    println(userData.first)

    // can split up the data by line
    val userFields = userData.map(line => line.split("\\|"))

    val numUsers = userFields.map(fields => fields(0)).count
    println(s"number of users: $numUsers")

    val numGenders = userFields.map(fields => fields(2)).distinct.count
    println(s"number of genders $numGenders")

    // you get the idea


    val ages = userFields.map(f => f(1).toInt).collect

    // this looks a bit shit, but at least it appears
    val f = Figure()
    val histo = f.subplot(2, 1, 1)
    histo += hist(ages)
    histo.title = "no idea if this will work"
    f.saveas("subplots.png")
  }
}
