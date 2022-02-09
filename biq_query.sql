-- Query from public tables
-- Select station_id,name,short_name from bigquery-public-data.new_york_citibike.citibike_stations Limit 100;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `supple-snow-338713.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_supple-snow-338713/raw/yellow_tripdata_2019-*.parquet', 'gs://dtc_data_lake_supple-snow-338713/raw/yellow_tripdata_2020-*.parquet']
);

-- Check yellow trip data
SELECT * FROM `supple-snow-338713.nytaxi.external_yellow_tripdata` limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `supple-snow-338713.nytaxi.yellow_tripdata_non_partitoned` AS
SELECT * FROM `supple-snow-338713.nytaxi.external_yellow_tripdata`;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `supple-snow-338713.nytaxi.yellow_tripdata_partitoned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `supple-snow-338713.nytaxi.external_yellow_tripdata`;

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `supple-snow-338713.nytaxi.yellow_tripdata_partitoned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `supple-snow-338713.nytaxi.external_yellow_tripdata`;

-- Creating external table referring to gcs path for fhv data
CREATE OR REPLACE EXTERNAL TABLE `supple-snow-338713.nytaxi.external_fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_supple-snow-338713/raw/fhv_tripdata_2019-*.parquet']
);

select * from `supple-snow-338713.nytaxi.external_fhv_tripdata` LIMIT 50;

--Count of distinct dispatching_base_num
select count(distinct dispatching_base_num) from `supple-snow-338713.nytaxi.external_fhv_tripdata`;

-- Creating a partition and cluster table for fhv data
CREATE OR REPLACE TABLE `supple-snow-338713.nytaxi.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `supple-snow-338713.nytaxi.external_fhv_tripdata`;

--count,actual and estimated
select count(*) 
from `supple-snow-338713.nytaxi.fhv_tripdata_partitoned_clustered`
where DATE(dropoff_datetime) between '2019-01-01' and '2019-03-31'
and dispatching_base_num in ('B00987', 'B02060', 'B02279');

