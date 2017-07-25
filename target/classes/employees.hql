CREATE TABLE employee ( id STRING, firstName STRING, lastName STRING, addresses  ARRAY < STRUCT < street:STRING,  city:STRING, state:STRING > > )  STORED AS PARQUET;