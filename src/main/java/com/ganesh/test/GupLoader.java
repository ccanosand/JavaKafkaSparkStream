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

        Dataset<Row> gupDataset = sparkSession.read()
                .option("inferSchema", "false")
                .schema(GUPStructure.getGupSchema())
                .json("gupProfiles.json");

        gupDataset.printSchema();
        gupDataset.createOrReplaceTempView("gupView");
/*
        Dataset<Row> deviceSettingsRecords =
                sqlCtx.sql("select payload.deviceSettings from gupView " );
        deviceSettingsRecords.show();*/

        Dataset<Row> explodedRecords = gupDataset.withColumn( "deviceSetting",
                org.apache.spark.sql.functions.explode(gupDataset.col("payload.deviceSettings")));
        explodedRecords.createOrReplaceTempView("explodedDeviceSettingsView");
        sqlCtx.sql("select deviceSetting.deviceId, deviceSetting.gupId, deviceSetting.settingName, " +
                "deviceSetting.settingValue from explodedDeviceSettingsView")
                .write().format("com.databricks.spark.csv")
                .option("header","true")
                .save("deviceSettings.csv");

        /*Dataset<Row> explodedRecords = gupDataset.withColumn( "deviceSetting",
                org.apache.spark.sql.functions.explode(gupDataset.col("payload.deviceSettings")));
        explodedRecords.createOrReplaceTempView("explodedDeviceSettingsView");

        sqlCtx.sql("select * from explodedDeviceSettingsView").
                write().format("com.databricks.spark.csv").save("table1.csv");*/

        /*Encoder<GupRecord> gupRecordEncoder = Encoders.bean(GupRecord.class);

        Dataset<GupRecord>  rowDataset = sparkSession.read().json("gupProfiles.json").as(gupRecordEncoder);
        //Dataset<GupRecord> rowDataset = sqlCtx.jsonFile("gupProfiles.json").as(gupRecordEncoder);
        rowDataset.show();

        rowDataset.createOrReplaceTempView("gupView");

        sqlCtx.sql("select payload.presets, payload.gupRecentPlaysCreateRequests from gupView").show();*/

        sparkSession.close();

    }

}


