package com.md.spark.sql.streaming

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataframeFilter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Spark Structured Streaming Unit Testing")
      .getOrCreate()

    print("hello  ")
    var personDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "yest1.fyre.ibm.com:9092")
      .option("subscribe", "person.event")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as value")

    /*** {"name":"donna","address":"India","department":"finance"} */

    val schema = StructType(List(
      StructField("name", StringType, true),
      StructField("address", StringType, true),
      StructField("department", StringType, true)))

    personDF = personDF.select(from_json(col("value"), schema).as("person")).select("person.*")

    personDF = streamFiltering(personDF)

    val query = personDF.writeStream
      .outputMode("update")
      .option("checkpointLocation", "/store/checkpoint/filter")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }


  case class Person(name : String, address: String, department : String)



  // Return the dataFrame having department marketing by using filter API
  def streamFiltering(inputDF: DataFrame): DataFrame = {

    inputDF.filter("department LIKE 'marketing'")

  }

}

