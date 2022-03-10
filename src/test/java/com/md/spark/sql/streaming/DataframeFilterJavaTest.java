package com.md.spark.sql.streaming;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import static junit.framework.TestCase.assertEquals;
import com.md.spark.sql.streaming.entity.Person;

public class DataframeFilterJavaTest {

    private static String sparkMaster = "local[4]";
    private static String checkpointLocation = "/Users/mayuri/maas/EventDriven/cp";

    private static SparkSession spark = SparkSession.builder()
            .appName("Event streams Java Unit Test")
            .master(sparkMaster)
            .getOrCreate();


    @BeforeAll
    public static void setUpClass() throws Exception {

    }

    @Test
    @DisplayName("Filtering the Input Stream should get filtered correctly")
    void testStreamFiltering() throws TimeoutException {

        Option<Object> intValue = Option.apply(1);
        // Some<Object> intValue = new Some<Object>(Integer.valueOf(1));
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        MemoryStream<Person> input = new MemoryStream<Person>(1, spark.sqlContext(), intValue, personEncoder);

        String jsonPath = "src/test/resources/data.json";
        // read JSON file to Dataset
        Dataset<Person> personEventDataset = spark.read().json(jsonPath).as(personEncoder);

        input.addData(JavaConverters.asScalaIteratorConverter(personEventDataset.collectAsList().iterator()).asScala().toSeq());

        Dataset<Person> sessions = input.toDS();


        Dataset filteredDF = DataframeFilterJava.streamFiltering(sessions.toDF());

        StreamingQuery streamingQuery = filteredDF
                .writeStream()
                .format("memory")
                .queryName("device")
                .outputMode("append")
                .start();


        streamingQuery.processAllAvailable();

        List<Row> result = spark.sql("select * from device").collectAsList();

        assertEquals("Filtering the Input Stream should get filtered correctly",2, result.size());

        assertEquals(RowFactory.create("India", "marketing", "bella"), result.get(0));


        // add more data
        List<Person> inputData = Arrays.asList(Person.newInstance("donna","India","finance"),Person.newInstance("dora","India","marketing"), Person.newInstance("dina","India","marketing"));


        input.addData(JavaConverters.asScalaBuffer(inputData).toSeq());

        streamingQuery.processAllAvailable();

        result = spark.sql("select * from device").collectAsList();

        assertEquals("Filtering the Input Stream should get filtered correctly",4, result.size());


    }

}
