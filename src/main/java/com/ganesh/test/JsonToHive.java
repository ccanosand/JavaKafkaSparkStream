package com.ganesh.test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonToHive {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonToHive.class);


    public static void main(String[] args) {
        String master = "local[*]";

        SparkSession sparkSession = SparkSession
                .builder()
                .appName(JsonToHive.class.getName())
                //.enableHiveSupport()
                .master(master)
                .getOrCreate();


        //SparkConf conf = new SparkConf().setAppName(JsonToHive.class.getName()).setMaster(master);
        //JavaSparkContext context = new JavaSparkContext(conf);
        SparkContext context = sparkSession.sparkContext();
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = sparkSession.sqlContext();
        Dataset<Row> rowDataset = sqlCtx.jsonFile("employees.json");
        rowDataset.printSchema();
        rowDataset.registerTempTable("employeesData");

        Dataset<Row> firstRow = sqlCtx.sql("select employee.firstName, employee.addresses from employeesData");
        firstRow.show();

        //firstRow.write().saveAsTable("employee");
        sparkSession.close();

    }


}


