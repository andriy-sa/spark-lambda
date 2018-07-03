package simple_job

import org.apache.spark.sql.SparkSession



object DemoJob {
  def main(args: Array[String]) : Unit = {
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .getOrCreate()

      val text_file = sc.sparkContext.textFile("/home/andy/test_resources/text.txt")

      val rdd = text_file.flatMap(line => line.toString.split(" "))
        .map( word => (word, 1))
        .reduceByKey{case (x, y) => x + y}
        .sortBy(x => x._2, false)

      rdd.foreach(x => println(x))
  }
}
