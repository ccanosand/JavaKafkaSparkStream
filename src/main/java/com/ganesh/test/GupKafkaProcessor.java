package com.ganesh.test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.ProcessingTime;

import java.util.UUID;
import java.util.regex.Pattern;

public final class GupKafkaProcessor {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        String brokers = "quickstart:9092";
        String topics = "ganesh1";
        String master = "local[*]";

        SparkSession sparkSession = SparkSession
                .builder().appName(GupLoader.class.getName())
                .master(master).getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        sparkContext.setLogLevel("ERROR");
        SQLContext sqlContext = sparkSession.sqlContext();

        Dataset<Row> rawDataSet = sparkSession.readStream().format("kafka").option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topics).load();
        rawDataSet = rawDataSet.withColumn("strValue", rawDataSet.col("value").cast("string"));
        rawDataSet = rawDataSet.withColumn("guprecord",
                functions.from_json(rawDataSet.col("strValue"),GUPStructure.getGupSchema()));
        rawDataSet.createOrReplaceTempView("gupView");

        createGupDataset(sqlContext);
        createPayloadDataset(rawDataSet, sqlContext);
        createDeviceSettingsDataset(rawDataSet, sqlContext);
        createGlobalSettingsDataset(rawDataSet, sqlContext);
        createPresetsDataset(rawDataSet, sqlContext);
        createProfileInfosDataset(rawDataSet, sqlContext);
        createGupRecentPlaysCreateRequestsDataset(rawDataSet, sqlContext);

        Dataset<Row> processedDataset = sqlContext.sql("select 1 from gupView");
        processedDataset.writeStream()
                .format("console")
                .trigger(ProcessingTime.create("10 seconds"))
                .start()
                .awaitTermination();
    }

    public static void createGupDataset(SQLContext sqlCtx) {

        sqlCtx.sql("select  "
                + "  guprecord.gupId ,   "
                + "  guprecord.actionDateTime ,  "
                + "  guprecord.dealerDemo ,  "
                + "  guprecord.deviceId ,  "
                + "  guprecord.action  ,  "
                + "  guprecord.payloadName  "
                + " from gupView")
                .writeStream()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("path", "output/gupDataset")
                .option("checkpointLocation", "cp/" + UUID.randomUUID().toString())
                .trigger(ProcessingTime.create("10 seconds"))
                .start();
    }

    public static void createPayloadDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        sqlCtx.sql("select  "
                + "  guprecord.payload.active ,  "
                + "  guprecord.payload.alertType ,  "
                + "  guprecord.payload.assetGUID ,  "
                + "  guprecord.payload.count ,  "
                + "  guprecord.payload.deviceId ,  "
                + "  guprecord.payload.lastSelectedTimestamp ,  "
                + "  guprecord.payload.legacyId1 ,  "
                + "  guprecord.payload.legacyId2 ,  "
                + "  guprecord.payload.locationId ,  "
                + "  guprecord.payload.moduleArea ,  "
                + "  guprecord.payload.moduleGUID ,  "
                + "  guprecord.payload.moduleType ,  "
                + "  guprecord.payload.gupId ,  "
                + "  guprecord.payload.stateData ,  "
                + "  guprecord.payload.storageRevision  "
                + " from gupView")
                .writeStream()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("path", "output/gupPayload")
                .option("checkpointLocation", "cp/" + UUID.randomUUID().toString())
                .trigger(ProcessingTime.create("10 seconds"))
                .start();
    }

    public static void createDeviceSettingsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {


        Dataset<Row> explodedRecords = gupDataset.withColumn("deviceSetting",
                org.apache.spark.sql.functions.explode(gupDataset.col("guprecord.payload.deviceSettings")));
        explodedRecords.createOrReplaceTempView("explodedDeviceSettingsView");
        sqlCtx.sql("select deviceSetting.deviceId, deviceSetting.gupId, deviceSetting.settingName, " +
                "deviceSetting.settingValue from explodedDeviceSettingsView")
                .writeStream()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("path", "output/deviceSettings")
                .option("checkpointLocation", "cp/" + UUID.randomUUID().toString())
                .trigger(ProcessingTime.create("10 seconds"))
                .start();
    }

    public static void createGlobalSettingsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("globalSetting",
                org.apache.spark.sql.functions.explode(gupDataset.col("guprecord.payload.globalSettings")));
        explodedRecords.createOrReplaceTempView("explodedGlobalSettingsView");
        sqlCtx.sql("select globalSetting.gupId, globalSetting.settingName, " +
                "globalSetting.settingValue from explodedGlobalSettingsView")
                .writeStream()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("path", "output/globalSettings")
                .option("checkpointLocation", "cp/" + UUID.randomUUID().toString())
                .trigger(ProcessingTime.create("10 seconds"))
                .start();
    }

    public static void createPresetsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("preset",
                org.apache.spark.sql.functions.explode(gupDataset.col("guprecord.payload.presets")));
        explodedRecords.createOrReplaceTempView("explodedPresetsView");

        sqlCtx.sql("select guprecord.gupId, preset.assetGUID, " +
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
                .writeStream()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("path", "output/presets")
                .option("checkpointLocation", "cp/" + UUID.randomUUID().toString())
                .trigger(ProcessingTime.create("10 seconds"))
                .start();
    }

    public static void createProfileInfosDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("profileInfo",
                org.apache.spark.sql.functions.explode(gupDataset.col("guprecord.payload.profileInfos")));
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
                .writeStream()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("path", "output/profileInfo")
                .option("checkpointLocation", "cp/" + UUID.randomUUID().toString())
                .trigger(ProcessingTime.create("10 seconds"))
                .start();
    }

    public static void createGupRecentPlaysCreateRequestsDataset(Dataset<Row> gupDataset, SQLContext sqlCtx) {

        Dataset<Row> explodedRecords = gupDataset.withColumn("gupRecentPlaysCreateRequest",
                org.apache.spark.sql.functions.explode(gupDataset.col("guprecord.payload.gupRecentPlaysCreateRequests")));
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
                .writeStream()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("path", "output/gupRecentPlaysCreateRequest")
                .option("checkpointLocation", "cp/" + UUID.randomUUID().toString())
                .trigger(ProcessingTime.create("10 seconds"))
                .start();
    }
}