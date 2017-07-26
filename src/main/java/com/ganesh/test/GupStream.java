package com.ganesh.test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class GupStream {


    private static final Logger LOGGER = LoggerFactory.getLogger(GupStream.class);

    public static void main(String[] args) throws AnalysisException {
        String master = "local[*]";

        SparkSession sparkSession = SparkSession
                .builder().appName(GupLoader.class.getName())
                .master(master).getOrCreate();

        SparkContext context = sparkSession.sparkContext();
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = sparkSession.sqlContext();

        Dataset<Row> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "quickstart:9092")
                .option("subscribe", "ganesh1")
                .load();
        ds1.printSchema();

        ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        ds1.show();

        /*ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        /*        .as[(String, String)]*/

    }

}
