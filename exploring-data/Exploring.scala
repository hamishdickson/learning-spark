package exploring

import org.apache.spark.SparkContext
import com.quantifind.charts.Highcharts._

import scala.util.{Try,Success,Failure}

/**
  * To run this just use `sbt run` - there is a bug in sbt (I think) when you use `run` where it doesn't shut something
  *  down and it kills your pc. I'm using 3rd party deps here for the charts, so sending it to spark is a pain
  *  because it needs those jars (it's doable... but it's easier to do sbt run...)
  */
object Exploring {
  def main(args: Array[String]): Unit = {
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

    bar(yAxis1.toList)

    val countByOccupation2 = userFields.map(f => f(3)).countByValue()
    println(s"map reduce appoach $countByOccupation2")
    println(s"count by value approach $countByOccupation")


    val movieData = sc.textFile("../data/ml-100k/u.item")

    val numMovies = movieData.count()

    // turns out there is some bad data in here, lets clean it up
    def convertYear(y: String): Int = {
      val year = y.split("-")
      val result = Try(year(2).toInt)

      result match {
        case Success(v) => v
        case Failure(_) => 1900 // just assign 1900
      }
    }

    val movieFields = movieData.map(lines => lines.split(("\\|")))

    val years = movieFields.map(f => convertYear(f(2)))

    val yearsFiltered = years.filter(_ != 1900)

    val movieAges = years.map(1998 - _).countByValue()

    val values = movieAges.values
    val bins = movieAges.keys

    histogram(values, 40) // guh - can't get this to work

    val ratingDataRaw = sc.textFile("../data/ml-100k/u.data")
    println(ratingDataRaw.first())

    val numRatings = ratingDataRaw.count()
    println(s"Ratings $numRatings")

    // annoyingly, this is tab separated
    val ratingData = ratingDataRaw.map(_.split("""\t"""))
    val ratings = ratingData.map(_(2).toInt)
    val maxRating = ratings.reduce(_ max _)
    val minRating = ratings.reduce(_ min _)
    val meanRating = ratings.reduce(_ + _) / numRatings

    val ratingsPerUser = numRatings / numUsers
    val ratingsPerMovie = numRatings / numMovies

    // this is cool - gives basically all these stats anyway
    println(ratings.stats())


    val userRatingsGrouped = ratingData.map(f => (f(0), f(2))).groupByKey()
    val userRatingsByUser = userRatingsGrouped.map((k,v) => (k,v.length))
  }
}
