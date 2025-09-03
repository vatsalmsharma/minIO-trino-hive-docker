CREATE SCHEMA minio.test
WITH (location = 's3a://test/');

CREATE TABLE minio.test.customer
WITH (
    format = 'ORC',
    external_location = 's3a://test/customer/'
) 
AS SELECT * FROM tpch.tiny.customer;

select count(*) from minio.test.customer;


-- Create bucket name 'datasets' for Hive
CREATE SCHEMA minio.datasets
WITH (location = 's3a://datasets/');

CREATE TABLE minio.datasets.nyc_taxi_zone (
    location_id VARCHAR,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR
)
WITH (
    external_location = 's3a://datasets/taxi_zone_lookup/',
    format = 'CSV',
    skip_header_line_count = 1
);


-- Iceberg 
CREATE SCHEMA iceberg.icedata
WITH (location = 's3a://icedata/');

use iceberg.icedata;

CREATE TABLE nyc_taxi_zone (
    location_id VARCHAR,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR
)
WITH (
    location = 's3a://icedata/taxi_zone_lookup',  
    format = 'PARQUET'
);


show tables in minio.datasets;

show tables in iceberg.icedata;

insert into iceberg.icedata.nyc_taxi_zone
select * from minio.datasets.nyc_taxi_zone;

-- validtion 
select count(*) from minio.datasets.nyc_taxi_zone;
select count(*) from iceberg.icedata.nyc_taxi_zone;


-- Example CTAS
CREATE TABLE hive.default.taxi_zone_lookup_iceberg
WITH (
  format = 'iceberg',
  location = 's3a://icedata/taxi_zone_lookup/'
) AS
SELECT * FROM hive.default.nyc_taxi_zone;


--------------
CREATE SCHEMA hive.poc
WITH (location = 's3a://hive/');

CREATE SCHEMA iceberg.poc
WITH (location = 's3a://iceberg/');


SELECT *
FROM TABLE (
    read_parquet(
        's3a://datasets/parquet_files/yellow_tripdata_2025-01.parquet'
    )
)
Limit 10;

SELECT *
FROM parquet."s3a://datasets/parquet_files/";

