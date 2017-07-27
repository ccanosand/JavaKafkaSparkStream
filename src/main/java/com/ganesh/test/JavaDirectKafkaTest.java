package com.ganesh.test;

import com.sun.prism.PixelFormat;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import org.apache.spark.sql.functions.*;

import java.util.*;
import java.util.regex.Pattern;

public final class JavaDirectKafkaTest {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        String brokers = "quickstart:9092";
        String topics = "ganesh1";
        String master = "local[*]";

        // Create context with a 2 seconds batch interval
       /* SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaTest").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        jssc.sparkContext().setLogLevel("ERROR");*/

        SparkSession sparkSession = SparkSession
                .builder().appName(GupLoader.class.getName())
                .master(master).getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        sparkContext.setLogLevel("ERROR");
        SQLContext sqlContext = sparkSession.sqlContext();

        Dataset<Row> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topics)
                .load();

        ds1.printSchema();
        ds1.createOrReplaceTempView("kafkaView");
        Dataset<Row> ds2 = sqlContext.sql("select key, cast( value as string ) as strValue from kafkaView");

        Dataset<Row> ds3 = ds2.withColumn("newColumns", functions.from_json(
                ds2.col("strValue"),
                EmployeeMessageStructure.getEmployeeSchema()));
        ds3.createOrReplaceTempView("updatedView");
        Dataset<Row> ds4 = sqlContext.sql("select key, strValue, newColumns.firstName, newColumns.lastName" +
                " from updatedView");

        //org.apache.spark.sql.functions.e

/*        Dataset<Row> ds3 = ds2.select(
                functions.from_json(
                        ds2.col("strValue"),
                        EmployeeMessageStructure.getEmployeeSchema()), ds2.col("newColumn"));*/

        ds4.writeStream()
                .format("console")
                .trigger(ProcessingTime.create("10 seconds"))
                .start()
                .awaitTermination();

        //ds1.printSchema();

        /*ds1.createOrReplaceTempView("ganesh");
        sqlContext.sql("select * from ganesh").show();*/

        //ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show();
    }
}