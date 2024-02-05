CREATE OR REPLACE EXTERNAL TABLE `dogwood-site-411520.dezoomcamp_hw3.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dogwood-site-411520-mage/green_tripdata_2022-*.parquet']
);

--1
SELECT
    COUNT(*)
FROM
    `dogwood-site-411520.dezoomcamp_hw3.external_green_tripdata`; --840402

--2
CREATE OR REPLACE TABLE `dogwood-site-411520.dezoomcamp_hw3.green_tripdata`
AS SELECT * FROM `dogwood-site-411520.dezoomcamp_hw3.external_green_tripdata`;

SELECT
    COUNT(DISTINCT(PULocationID))
FROM
   `dogwood-site-411520.dezoomcamp_hw3.external_green_tripdata`; --0b

SELECT
    COUNT(DISTINCT(PULocationID))
FROM
   `dogwood-site-411520.dezoomcamp_hw3.green_tripdata`; --6.41 mb

--3
SELECT
    COUNT(*)
FROM
    `dogwood-site-411520.dezoomcamp_hw3.green_tripdata`
WHERE
    fare_amount = 0; --1622

--4
CREATE OR REPLACE TABLE `dogwood-site-411520.dezoomcamp_hw3.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `dogwood-site-411520.dezoomcamp_hw3.external_green_tripdata`
);

--5
SELECT
    DISTINCT
    PULocationID
FROM
    `dogwood-site-411520.dezoomcamp_hw3.green_tripdata_partitioned`
WHERE
   DATE(lpep_pickup_datetime) >= '2022-06-01' AND DATE(lpep_pickup_datetime) <= '2022-06-30'; --1.12 MB

SELECT
    DISTINCT
    PULocationID
FROM
    `dogwood-site-411520.dezoomcamp_hw3.green_tripdata`
WHERE
   DATE(lpep_pickup_datetime) >= '2022-06-01' AND DATE(lpep_pickup_datetime) <= '2022-06-30'; --12.82 MB