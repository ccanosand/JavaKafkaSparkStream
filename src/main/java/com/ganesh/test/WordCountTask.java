package com.ganesh.test;

import org.apache.spark.InternalAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class WordCountTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);


    public static void main(String[] args) {
        new WordCountTask().run("employees.json");
    }

    public void run(String inputFilePath) {
        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName(WordCountTask.class.getName())
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = new SQLContext(context);
        Dataset<Row> rowDataset = sqlCtx.jsonFile(inputFilePath);
        rowDataset.printSchema();
        rowDataset.registerTempTable("employeesData");

        Dataset<Row> firstRow = sqlCtx.sql("select employee.firstName, employee.addresses from employeesData limit 1");
        firstRow.show();

        firstRow.write().saveAsTable("employee");

        context.close();

       /* JavaRDD<String> fileRDD = context.textFile(inputFilePath);

        fileRDD.flatMap(text -> Arrays.asList(text.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .foreach(result -> LOGGER.info(
                        String.format("Word [%s] count [%d].", result._1(), result._2)));*/
    }
}


