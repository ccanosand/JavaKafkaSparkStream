package com.ganesh.test;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class EmployeeMessageStructure {

    public static StructType getEmployeeSchema() {
        List<StructField> rootFields = createRootElements();
        StructType schema = DataTypes.createStructType(rootFields);
        return schema;
    }

    public static List<StructField> createRootElements() {

        List<StructField> rootFields = new ArrayList<>();

        rootFields.add(DataTypes.createStructField("firstName", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("lastName", DataTypes.StringType, true));

        return rootFields;
    }
}
