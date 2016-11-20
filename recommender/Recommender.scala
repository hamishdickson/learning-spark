package recommender

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ ALS, Rating, MatrixFactorizationModel }

object Recommender {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "recommender")

    // getting data and only taking the bits we really care about
    val rawData = sc.textFile("../data/ml-100k/u.data")
    rawData.first

    // tab seperated, and lets only take the first 3 columns - data cleaning
    val rawRatings = rawData.map(_.split("\t").take(3))

    // ALS stuff
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }

    // ok time to train the thing
    // rank: number of factors in our model - 10-200 is good
    // iterations: 10 is a decent default
    // lambda: helps stop overfitting - needs to be played with
    val model = ALS.train(ratings, 50, 10, 0.01)

    // user 789, film 123
    val predict = model.predict(798, 123)

    // lets generate the top 25 recommendated items for a user
    val userId = 789
    val K = 25
    val topKRecs = model.recommendProducts(userId, K)
    // ^^ this is cool

    val movies = sc.textFile("../data/ml-100k/u.item")

    val titles = movies.map(l => l.split("\\|").take(2)).map(a => (a(0).toInt, a(1))).collectAsMap()
  }
}
