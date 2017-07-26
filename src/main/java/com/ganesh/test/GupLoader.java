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

        createGupDataset(gupDataset, sqlCtx);
        createPayloadDataset(gupDataset, sqlCtx);
        createDeviceSettingsDataset(gupDataset, sqlCtx);
        createGlobalSettingsDataset(gupDataset, sqlCtx);
        createPresetsDataset(gupDataset, sqlCtx);
        createProfileInfosDataset(gupDataset, sqlCtx);
        createGupRecentPlaysCreateRequestsDataset(gupDataset, sqlCtx);

        sparkSession.close();
    }

    public static void createGupDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        sqlCtx.sql("select  "
                + "  gupId ,   "
                + "  actionDateTime ,  "
                + "  dealerDemo ,  "
                + "  deviceId ,  "
                + "  action  ,  "
                + "  payloadName  "
                + " from gupView")
                .write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save("gupSettings.csv");
    }

    public static void createPayloadDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        sqlCtx.sql("select  "
                + "  payload.active ,  "
                + "  payload.alertType ,  "
                + "  payload.assetGUID ,  "
                + "  payload.count ,  "
                + "  payload.deviceId ,  "
                + "  payload.lastSelectedTimestamp ,  "
                + "  payload.legacyId1 ,  "
                + "  payload.legacyId2 ,  "
                + "  payload.locationId ,  "
                + "  payload.moduleArea ,  "
                + "  payload.moduleGUID ,  "
                + "  payload.moduleType ,  "
                + "  payload.gupId ,  "
                + "  payload.stateData ,  "
                + "  payload.storageRevision  "
                + " from gupView")
                .write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save("gupPayloads.csv");
    }

    public static void createDeviceSettingsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("deviceSetting",
                org.apache.spark.sql.functions.explode(gupDataset.col("payload.deviceSettings")));
        explodedRecords.createOrReplaceTempView("explodedDeviceSettingsView");
        sqlCtx.sql("select deviceSetting.deviceId, deviceSetting.gupId, deviceSetting.settingName, " +
                "deviceSetting.settingValue from explodedDeviceSettingsView")
                .write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save("deviceSettings.csv");
    }

    public static void createGlobalSettingsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("globalSetting",
                org.apache.spark.sql.functions.explode(gupDataset.col("payload.globalSettings")));
        explodedRecords.createOrReplaceTempView("explodedGlobalSettingsView");
        sqlCtx.sql("select globalSetting.gupId, globalSetting.settingName, " +
                "globalSetting.settingValue from explodedGlobalSettingsView")
                .write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save("globalSettings.csv");
    }

    public static void createPresetsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("preset",
                org.apache.spark.sql.functions.explode(gupDataset.col("payload.presets")));
        explodedRecords.createOrReplaceTempView("explodedPresetsView");

        sqlCtx.sql("select gupId, preset.assetGUID, " +
                "preset.assetName , " +
                "preset.assetType , " +
                "preset.autoDownload , " +
                "preset.caId , " +
                "preset.changeType , " +
                "preset.channelId , " +
                "preset.createdDateTime , " +
                "preset.globalSortOrder , " +
                "preset.legacySortOrder , " +
                "preset.modifiedDateTime , " +
                "preset.showDateTime  " +
                "  from explodedPresetsView")
                .write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save("presets.csv");
    }

    public static void createProfileInfosDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("profileInfo",
                org.apache.spark.sql.functions.explode(gupDataset.col("payload.profileInfos")));
        explodedRecords.createOrReplaceTempView("explodedProfileInfosView");

        sqlCtx.sql("select " +
                "profileInfo.createdDateTime , " +
                "profileInfo.deleted , " +
                "profileInfo.encoding , " +
                "profileInfo.gupId , " +
                "profileInfo.modifiedDateTime , " +
                "profileInfo.name , " +
                "profileInfo.source , " +
                "profileInfo.value " +
                "  from explodedProfileInfosView")
                .write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save("profileInfos.csv");
    }

    public static void createGupRecentPlaysCreateRequestsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("gupRecentPlaysCreateRequest",
                org.apache.spark.sql.functions.explode(gupDataset.col("payload.gupRecentPlaysCreateRequests")));
        explodedRecords.createOrReplaceTempView("explodedGupRecentPlaysCreateRequestsView");

        sqlCtx.sql("select " +
                " gupRecentPlaysCreateRequest.recentPlay.aodDownload ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.aodPercentConsumed ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.assetGUID ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.assetType ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.channelId ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.deviceId ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.endDateTime ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.endStreamDateTime ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.endStreamTime ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.recentPlayId ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.recentPlayType ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.startDateTime ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.startStreamDateTime ,  " +
                " gupRecentPlaysCreateRequest.recentPlay.startStreamTime " +
                "  from explodedGupRecentPlaysCreateRequestsView")
                .write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save("gupRecentPlaysCreateRequests.csv");
    }
}


