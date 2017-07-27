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
        payLoadFields.add(DataTypes.createStructField("globalSettings", createGlobalSettingsElements(), true));
        payLoadFields.add(DataTypes.createStructField("presets", createPresetsElements(), true));
        payLoadFields.add(DataTypes.createStructField("profileInfos", createprofileInfoElements(), true));
        payLoadFields.add(DataTypes.createStructField("gupRecentPlaysCreateRequests", createGupRecentPlaysCreateRequestsElements(), true));


        StructType payloadStruct = DataTypes.createStructType(payLoadFields);
        return payloadStruct;
    }

    public static ArrayType createDeviceSettingsElements() {

        List<StructField> deviceSettingFields = new ArrayList<>();

        deviceSettingFields.add(DataTypes.createStructField("deviceId", DataTypes.StringType, true));
        deviceSettingFields.add(DataTypes.createStructField("gupId", DataTypes.StringType, true));
        deviceSettingFields.add(DataTypes.createStructField("settingName", DataTypes.StringType, true));
        deviceSettingFields.add(DataTypes.createStructField("settingValue", DataTypes.StringType, true));
        StructType deviceSettingStruct = DataTypes.createStructType(deviceSettingFields);

        ArrayType deviceSettingsStruct = DataTypes.createArrayType(deviceSettingStruct);
        return deviceSettingsStruct;

    }

    public static ArrayType createGlobalSettingsElements() {

        List<StructField> globalSettingFields = new ArrayList<>();

        globalSettingFields.add(DataTypes.createStructField("gupId", DataTypes.StringType, true));
        globalSettingFields.add(DataTypes.createStructField("settingName", DataTypes.StringType, true));
        globalSettingFields.add(DataTypes.createStructField("settingValue", DataTypes.StringType, true));
        StructType deviceSettingStruct = DataTypes.createStructType(globalSettingFields);

        ArrayType deviceSettingsStruct = DataTypes.createArrayType(deviceSettingStruct);
        return deviceSettingsStruct;

    }

    public static ArrayType createPresetsElements() {

        List<StructField> presetFields = new ArrayList<>();

        presetFields.add(DataTypes.createStructField("assetGUID", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("assetName", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("assetType", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("autoDownload", DataTypes.BooleanType, true));
        presetFields.add(DataTypes.createStructField("caId", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("changeType", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("channelId", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("createdDateTime", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("globalSortOrder", DataTypes.LongType, true));
        presetFields.add(DataTypes.createStructField("legacySortOrder", DataTypes.LongType, true));
        presetFields.add(DataTypes.createStructField("modifiedDateTime", DataTypes.StringType, true));
        presetFields.add(DataTypes.createStructField("showDateTime", DataTypes.StringType, true));

        StructType presetStruct = DataTypes.createStructType(presetFields);
        ArrayType presetsStruct = DataTypes.createArrayType(presetStruct);
        return presetsStruct;

    }

    public static StructType createRecentPlayStruct() {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("aodDownload", DataTypes.BooleanType, true));
        fields.add(DataTypes.createStructField("aodPercentConsumed", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("assetGUID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("assetName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("assetType", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("channelId", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("deviceId", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("endDateTime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("endStreamDateTime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("endStreamTime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("recentPlayId", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("recentPlayType", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("startDateTime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("startStreamDateTime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("startStreamTime", DataTypes.StringType, true));

        StructType recentPlayStruct = DataTypes.createStructType(fields);
        return recentPlayStruct;
    }

    public static ArrayType createGupRecentPlaysCreateRequestsElements() {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("recentPlay", createRecentPlayStruct(), true));

        StructType struct = DataTypes.createStructType(fields);
        ArrayType arrayStruct = DataTypes.createArrayType(struct);
        return arrayStruct;

    }

    public static ArrayType createprofileInfoElements() {

        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("createdDateTime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("deleted", DataTypes.BooleanType, true));
        fields.add(DataTypes.createStructField("encoding", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("gupId", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("modifiedDateTime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));

        StructType struct = DataTypes.createStructType(fields);
        ArrayType arrayStruct = DataTypes.createArrayType(struct);
        return arrayStruct;

    }

}
