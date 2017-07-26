package com.ganesh.test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.datatype.DatatypeConfigurationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EmployeeLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeLoader.class);


    public static void main(String[] args) throws AnalysisException {
        String master = "local[*]";

        List<StructField> employeeFields = new ArrayList<>();
        employeeFields.add(DataTypes.createStructField("firstName", DataTypes.StringType, true));
        employeeFields.add(DataTypes.createStructField("lastName", DataTypes.StringType, true));
        employeeFields.add(DataTypes.createStructField("email", DataTypes.StringType, true));

        List<StructField> addressFields = new ArrayList<>();
        addressFields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        addressFields.add(DataTypes.createStructField("state", DataTypes.StringType, true));
        addressFields.add(DataTypes.createStructField("zip", DataTypes.StringType, true));

        ArrayType addressStruct = DataTypes.createArrayType( DataTypes.createStructType(addressFields));

        //StructType addressStruct = DataTypes.createStructType(addressFields);

        employeeFields.add(DataTypes.createStructField("addresses", addressStruct, true));

        //employeeFields.add(DataTypes.createStructField("addresses", DataTypes.createStructType(addressFields), true));

        StructType employeeSchema = DataTypes.createStructType(employeeFields);

        SparkSession sparkSession = SparkSession
                .builder().appName(SaveToCSV.class.getName())
                .master(master).getOrCreate();

        SparkContext context = sparkSession.sparkContext();
        context.setLogLevel("ERROR");

        SQLContext sqlCtx = sparkSession.sqlContext();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);

        Dataset<Employee>  rowDataset = sparkSession.read()
                .option("inferSchema", "false")
                .schema(employeeSchema)
                .json("simple_employees.json").as(employeeEncoder);
        //Dataset<GupRecord> rowDataset = sqlCtx.jsonFile("gupProfiles.json").as(gupRecordEncoder);
        //rowDataset.show();

        rowDataset.createOrReplaceTempView("employeeView");

        sqlCtx.sql("select * from employeeView").show();

        sparkSession.close();

    }

}


