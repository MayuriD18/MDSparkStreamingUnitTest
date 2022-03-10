package com.md.spark.sql.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Serializable;

import java.util.Map;

public class DataframeFilterJava {

    // Return the dataFrame having department marketing by using filter API
    public static Dataset<Row> streamFiltering(Dataset<Row> inputDF){

       return inputDF.filter("department LIKE 'marketing'");

    }

}
