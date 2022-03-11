package com.md.spark.sql.streaming

import com.google.gson.{JsonIOException, JsonSyntaxException}
import org.apache.spark.sql.{Row, RowFactory, SQLContext, SparkSession}
import org.junit.jupiter.api.{BeforeEach, DisplayName, Test}
import com.md.spark.sql.streaming.DataframeFilter.Person
import junit.framework.TestCase.assertEquals
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.junit.Assert

import java.io.FileNotFoundException
import java.util
import java.util.List

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
  def testStreamFiltering(): Unit = {

    val inputData = Seq(Person("donna","india","finance"), Person("dora","india","marketing"), Person("dina","india","marketing"))

    import org.apache.spark.sql.{Encoder, Encoders}
    implicit val personEncoder: Encoder[Person] = Encoders.product[Person]

    val inputStream: MemoryStream[Person] = new MemoryStream[Person](1, spark.sqlContext,Some(5))
    implicit val sqlCtx: SQLContext = spark.sqlContext
    val sessions = inputStream.toDF()

    val filteredDF = DataframeFilter.streamFiltering(sessions)

    val streamingQuery = filteredDF.writeStream.format("memory").queryName("person").outputMode("append").start

    // add data to the query in streaming way
    inputStream.addData(inputData)

    // process the data which we added
    streamingQuery.processAllAvailable()

    val result = spark.sql("select * from person").collectAsList

    assertEquals("Filtering the Input Stream should get filtered correctly", 2, result.size)

    assertEquals(RowFactory.create("dora", "india", "marketing"), result.get(0))

  }
}
