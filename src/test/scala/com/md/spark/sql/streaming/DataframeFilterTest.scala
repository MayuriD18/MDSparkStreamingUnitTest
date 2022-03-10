package com.md.spark.sql.streaming

import com.google.gson.{JsonIOException, JsonSyntaxException}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.jupiter.api.{BeforeEach, DisplayName, Test}
import com.md.spark.sql.streaming.DataframeFilter.Person
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.junit.Assert

import java.io.FileNotFoundException

class DataframeFilterTest {

  val sparkMaster = "local[4]"


  val spark = SparkSession.builder()
    .master(sparkMaster)
    .appName("Event streams Scala Unit Test")
    .getOrCreate()


  @BeforeEach
  @throws[Exception]
  def setUp(): Unit = {

  }



  @Test
  @DisplayName("Filtering the Input Stream should get filtered correctly")
  @throws[JsonSyntaxException]
  @throws[JsonIOException]
  @throws[FileNotFoundException]
  def testStreamFiltering(): Unit = {


    val inputData = Seq(Person("donna","india","finance"),
      Person("dora","india","marketing"),
      Person("dina","india","marketing"))

    // we can use json here

    import org.apache.spark.sql.{Encoder, Encoders}
    implicit val personEncoder: Encoder[Person] = Encoders.product[Person]

    var inputStream: MemoryStream[Person] = new MemoryStream[Person](1, spark.sqlContext,Some(5))

    implicit val sqlCtx: SQLContext = spark.sqlContext

    val sessions = inputStream.toDS

    // calling out business logic which needs to be unit tested
    val filteredDF = DataframeFilter.streamFiltering(sessions.toDF())

    val streamingQuery = filteredDF
      .writeStream
      .format("memory")
      .queryName("person")
      .outputMode("append")
      .start

    // add data to the query in streaming way
    val currentOffset = inputStream.addData(inputData)

    // process the data which we added
    streamingQuery.processAllAvailable()

    inputStream.commit(currentOffset.asInstanceOf[LongOffset])

    val outputSize = spark.sql("select count(*) from person")
      .collect()
      .map(_.getAs[Long](0))
      .head

    Assert.assertEquals(2,outputSize)

  }
}
