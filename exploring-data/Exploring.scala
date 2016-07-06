package exploring

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.quantifind.charts.Highcharts._

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

    val numUsers = userFields.map(fields => fields(0)).count     // <- warning! array!
    println(s"number of users: $numUsers")

    val numGenders = userFields.map(fields => fields(2)).distinct.count
    println(s"number of genders $numGenders")

    // you get the idea


    val ages = userFields.map(f => f(1).toInt).collect


    histogram(ages, 20)
    title("Ages")

    // note: collect() doesn't return in a specific order
    val countByOccupation = userFields.map(f => (f(3), 1)).reduceByKey(_ + _).collect

    val occupations: Array[String] = countByOccupation.map{ case (x, y) => x }
    val counts: Array[Int] = countByOccupation.map{ case (x, y) => y }

    val z = occupations zip counts

    val orderedZ = z.sortBy(_._2)

    val xAxis1 = orderedZ.map{ case (x, y) => x }
    val yAxis1 = orderedZ.map{ case (x, y) => y }


    
  }
}
