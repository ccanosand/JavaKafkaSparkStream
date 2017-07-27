package com.ganesh.test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.ProcessingTime;

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
                functions.from_json(
                        rawDataSet.col("strValue"),
                        GUPStructure.getGupSchema()));
        //EmployeeMessageStructure.getEmployeeSchema()));
        rawDataSet.createOrReplaceTempView("processedView");
        Dataset<Row> processedDataset = sqlContext.sql("select * from processedView");

        processedDataset.writeStream()
                .format("console").trigger(ProcessingTime.create("10 seconds")).start()
                .awaitTermination();
    }
}