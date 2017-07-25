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

public class SaveToCSV {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveToCSV.class);


    public static void main(String[] args) throws AnalysisException {
        String master = "local[*]";

        SparkSession sparkSession = SparkSession
                .builder().appName(SaveToCSV.class.getName())
                .master(master).getOrCreate();

        SparkContext context = sparkSession.sparkContext();
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = sparkSession.sqlContext();

        Dataset<Row> rowDataset = sqlCtx.jsonFile("employees.json");
        rowDataset.printSchema();
        rowDataset.createOrReplaceTempView("employeesData1");

        sqlCtx.udf().register("idGenerator", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return UUID.randomUUID().toString();
            }
        }, DataTypes.StringType);


        sqlCtx.udf().register("arrayConvertor", new UDF1<WrappedArray<Object>, String>() {
            @Override
            public String call(WrappedArray<Object> addresses) throws Exception {
                System.out.println(addresses);
                Set<String> addrs = new HashSet<>();
                Iterator<Object> objectIterator = addresses.iterator();
                while (objectIterator.hasNext()) {
                    Object addrObject = objectIterator.next();
                    addrs.add(addrObject.toString());
                }
                return Arrays.toString(addrs.toArray());
            }
        }, DataTypes.StringType);

        sqlCtx.udf().register("objectConvertor", new UDF1<Object, String>() {
            @Override
            public String call(Object address) throws Exception {
                System.out.println(address.toString());
                return address.toString();
            }
        }, DataTypes.StringType);


        Dataset<Row> allRecords = sqlCtx.sql("select idGenerator(employee.firstname) as id, employee.firstname, " +
                "employee.lastName, employee.addresses from employeesData1");
        allRecords.createOrReplaceTempView("allRecordsView");

        Dataset<Row> explodedRecords = allRecords.withColumn( "address",
                org.apache.spark.sql.functions.explode(allRecords.col("addresses")));
        explodedRecords.createOrReplaceTempView("explodedView");

        sqlCtx.sql("select id, firstname, lastname from allRecordsView").
                write().format("com.databricks.spark.csv").save("table1.csv");

        sqlCtx.sql("select id, objectConvertor(address) from explodedView").
                write().format("com.databricks.spark.csv").save("table2.csv");

        sparkSession.close();
    }

}


