package com.ganesh.test;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class KafkaMessageStructure {

    public static StructType getKafkaSchema() {
        List<StructField> rootFields = createRootElements();
        StructType schema = DataTypes.createStructType(rootFields);
        return schema;
    }

    public static List<StructField> createRootElements() {

        List<StructField> rootFields = new ArrayList<>();

        rootFields.add(DataTypes.createStructField("key", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("topic", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("partition", DataTypes.IntegerType, true));
        rootFields.add(DataTypes.createStructField("offset", DataTypes.LongType, true));
        rootFields.add(DataTypes.createStructField("timestamp", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("timestampType", DataTypes.IntegerType, true));
        rootFields.add(DataTypes.createStructField("ganesh_field", DataTypes.IntegerType, true));

        return rootFields;
    }
}
