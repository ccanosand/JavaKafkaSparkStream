package com.ganesh.test;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class GUPStructure {

    public static StructType getGupSchema() {
        List<StructField> rootFields = createRootElements();
        //rootFields.add(DataTypes.createStructField("payload", createPayloadElements(), true));
        StructType gupSchema = DataTypes.createStructType(rootFields);
        return gupSchema;
    }

    public static List<StructField> createRootElements() {

        List<StructField> rootFields = new ArrayList<>();

        rootFields.add(DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("action", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("actionDateTime", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("dealerDemo", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("deviceId", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("gupId", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("payloadName", DataTypes.StringType, true));
        rootFields.add(DataTypes.createStructField("payload", createPayloadElements(), true));

        return rootFields;
    }

    public static StructType createPayloadElements() {

        List<StructField> payLoadFields = new ArrayList<>();

        payLoadFields.add(DataTypes.createStructField("active", DataTypes.BooleanType, true));
        payLoadFields.add(DataTypes.createStructField("alertType", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("assetGUID", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
        payLoadFields.add(DataTypes.createStructField("deviceId", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("lastSelectedTimestamp", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("legacyId1", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("legacyId2", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("locationId", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("moduleArea", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("moduleGUID", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("moduleType", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("gupId", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("stateData", DataTypes.StringType, true));
        payLoadFields.add(DataTypes.createStructField("storageRevision", DataTypes.StringType, true));

        payLoadFields.add(DataTypes.createStructField("deviceSettings", createDeviceSettingsElements(), true));

        StructType payloadStruct = DataTypes.createStructType(payLoadFields);
        return payloadStruct;
    }

    public static ArrayType createDeviceSettingsElements(){

        List<StructField> deviceSettingFields = new ArrayList<>();

        deviceSettingFields.add(DataTypes.createStructField("deviceId", DataTypes.StringType, true));
        deviceSettingFields.add(DataTypes.createStructField("gupId", DataTypes.StringType, true));
        deviceSettingFields.add(DataTypes.createStructField("settingName", DataTypes.StringType, true));
        deviceSettingFields.add(DataTypes.createStructField("settingValue", DataTypes.StringType, true));
        StructType deviceSettingStruct = DataTypes.createStructType(deviceSettingFields);

        //ArrayType addressStruct = DataTypes.createArrayType( DataTypes.createStructType(addressFields));
        //employeeFields.add(DataTypes.createStructField("addresses", addressStruct, true));
        ArrayType deviceSettingsStruct = DataTypes.createArrayType(deviceSettingStruct);
        return deviceSettingsStruct;

    }


}
