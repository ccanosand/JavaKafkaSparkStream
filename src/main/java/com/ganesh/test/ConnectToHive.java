package com.ganesh.test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ConnectToHive {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectToHive.class);


    public static void main(String[] args) throws AnalysisException {
        String master = "local[*]";

        SparkSession sparkSession = SparkSession
                .builder().appName(ConnectToHive.class.getName())
                //.config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")
                .enableHiveSupport()
                .master(master).getOrCreate();

        SparkContext context = sparkSession.sparkContext();
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = sparkSession.sqlContext();


        //HiveContext hiveContext = new HiveContext(sparkSession);
        //hiveContext.setConf("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:9000/user/hive/warehouse");

        sqlCtx.sql("SHOW DATABASES").show();
        sqlCtx.sql("SHOW TABLES").show();

        sparkSession.close();
    }


}


