package cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object DataGroup {
  def main(args: Array[String]) : Unit = {
    val sc = SparkSession.builder
      .master("local")
      .appName("Test cassandra")
      .config("spark.executor.memory", "1g")
      .config("spark.cores.max", "2")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra")
      .getOrCreate()

    import sc.implicits._

    val today = "2018-07-06"

    val today_orders = sc.read.format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "orders", "keyspace" -> "shop"))
      .load()
      .where($"date" === today)
      .cache()

    // get count, price grouped by product
    val statistic = today_orders.groupBy("product", "date")
      .agg(sum("count").as("count"), sum("price").as("price"))

    val total_count = today_orders.groupBy("date").agg(count("count").as("total_count"))

    statistic.write
      .format("org.apache.spark.sql.cassandra")
      .mode(SaveMode.Append)
      .options(Map("table" -> "product_statistic", "keyspace" -> "shop"))
      .save()

    total_count.write
      .format("org.apache.spark.sql.cassandra")
      .mode(SaveMode.Append)
      .options(Map("table" -> "day_statistic", "keyspace" -> "shop"))
      .save()
  }
}
