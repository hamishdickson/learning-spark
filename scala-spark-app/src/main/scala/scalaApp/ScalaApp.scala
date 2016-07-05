import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
  * A simple spark app in scala
  */
object ScalaApp {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val data = sc.textFile("data/UserPurchaseHistory.csv")
      .map(line => line.split(","))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

    // number of purchases
    val numPurchases = data.count

    // unique users who made purchases
    val uniqueUsers = data.map { case (user, product, price) => user }.distinct.count

    // total revenue
    val totalRevenue = data.map { case (user, product, price) => price.toDouble }.sum

    // most popular product
    val productByPopularity = data
      .map { case (user, product, price) => (product, 1) }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)

    val mostPopular = productByPopularity(0)

    println(s"total purchases: $numPurchases")
    println(s"unique users: $uniqueUsers")
    println(s"total revenue: $totalRevenue")
    println(s"most popular product: ${mostPopular._1} with ${mostPopular._2} purchases")

  }
}
