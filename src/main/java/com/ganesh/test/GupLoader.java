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

public class GupLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(GupLoader.class);


    public static void main(String[] args) throws AnalysisException {
        String master = "local[*]";

        SparkSession sparkSession = SparkSession
                .builder().appName(SaveToCSV.class.getName())
                .master(master).getOrCreate();

        SparkContext context = sparkSession.sparkContext();
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = sparkSession.sqlContext();

        /*Dataset<Row> rowDataset = sqlCtx.jsonFile("gupProfiles.json");
        rowDataset.printSchema();
        rowDataset.createOrReplaceTempView("gupView");

        sqlCtx.sql("select * from gupView").show();*/

        Encoder<GupRecord> gupRecordEncoder = Encoders.bean(GupRecord.class);

        Dataset<GupRecord>  rowDataset = sparkSession.read().json("gupProfiles.json").as(gupRecordEncoder);
        //Dataset<GupRecord> rowDataset = sqlCtx.jsonFile("gupProfiles.json").as(gupRecordEncoder);
        rowDataset.show();

        rowDataset.createOrReplaceTempView("gupView");

        sqlCtx.sql("select payload.presets, payload.gupRecentPlaysCreateRequests from gupView").show();

        sparkSession.close();

    }

}


