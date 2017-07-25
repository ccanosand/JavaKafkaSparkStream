package com.ganesh.test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;

public class JsonToHive {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonToHive.class);


    public static void main(String[] args) throws AnalysisException {
        String master = "local[*]";

        SparkSession sparkSession = SparkSession
                .builder().appName(JsonToHive.class.getName())
                //.config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")
                //.enableHiveSupport()
                .master(master).getOrCreate();

        SparkContext context = sparkSession.sparkContext();
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = sparkSession.sqlContext();

        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkSession);

        Dataset<Row> rowDataset = sqlCtx.jsonFile("employees.json");
        rowDataset.printSchema();
        rowDataset.createOrReplaceTempView("employeesData1"); //registerTempTable("employeesData");

        sqlCtx.udf().register("idGenerator", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return UUID.randomUUID().toString();
            }
        }, DataTypes.StringType);

        System.out.println(" ==================================================  ");

        //hiveContext.setConf("hive.metastore.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse");

        hiveContext.sql("SHOW DATABASES").show();
        hiveContext.sql("SHOW TABLES").show();

      /*  hiveContext.sql("SHOW COLUMNS FROM employee").show();
        sqlCtx.sql("SHOW COLUMNS FROM default.employee").show();*/

        System.out.println(" ==================================================  ");

        /*sparkSession.catalog().listTables().select("*").show();

        System.out.println(" ==================================================  ");

        sqlCtx.sql("SHOW COLUMNS FROM default.employee").show();

        System.out.println(" ==================================================  ");

        sqlCtx.sql("SHOW COLUMNS FROM employeesData1").show();

        System.out.println(" ==================================================  "); */

        // rowDataset.withColumn("employee.id", );
        Dataset<Row> firstRow = sqlCtx.sql("select idGenerator(employee.firstname) as id, employee.firstname, employee.lastName, employee.addresses from employeesData1");
        firstRow.show();


        /*System.out.println(sparkSession.catalog().getTable("employee").description());
        System.out.println(Arrays.toString(sparkSession.catalog().listTables().select("employeesdata1").columns()));*/

        //sparkSession.catalog().listTables().select("*").show();

        // sqlCtx.sql("insert into table default.employee  select idGenerator(employee.firstname), employee.firstname, employee.lastName, employee.addresses from employeesData1");

        //firstRow.write().mode("append").insertInto("default.employee");
        //firstRow.write().saveAsTable("default.employee");
        sparkSession.close();

    }


}


