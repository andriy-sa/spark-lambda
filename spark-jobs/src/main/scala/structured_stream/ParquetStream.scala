package structured_stream

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{from_json, col}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

object ParquetStream {
  def main(args: Array[String]) : Unit = {

    val sc = SparkSession.builder
      .master("local")
      .appName("Stream to Parquet")
      .config("spark.executor.memory", "1g")
      .config("spark.cores.max", "2")
      .getOrCreate()


    import org.apache.spark.sql.types._
    val schema =  StructType(
      StructField("rate", IntegerType) ::
        StructField("id", IntegerType) ::
        StructField("datetime", StringType) :: Nil
    )

    val df = sc.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "spark_kafka")
      .load()


    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(from_json(col("value").cast(StringType), schema).alias("json"), col("timestamp"))
      .select(col("json.*"), col("timestamp"))

    val query = data.writeStream
      .outputMode("append")
      .trigger(ProcessingTime("5 seconds"))
      .format("console")
      .start()

    query.awaitTermination()

  }
}
