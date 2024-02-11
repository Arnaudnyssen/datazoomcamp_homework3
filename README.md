# datazoomcamp_homework3
#upload the files in gcs bucket with the command line:

gsutil cp green_tripdata_2022-01.parquet gs://mage-zoomcamp-arnaudn/

gsutil cp green_tripdata_2022-02.parquet gs://mage-zoomcamp-arnaudn/

gsutil cp green_tripdata_2022-03.parquet gs://mage-zoomcamp-arnaudn/

gsutil cp green_tripdata_2022-04.parquet gs://mage-zoomcamp-arnaudn/

....

gsutil cp green_tripdata_2022-12.parquet gs://mage-zoomcamp-arnaudn/

# now this is all the sql queries in bigquery
CREATE OR REPLACE EXTERNAL TABLE ny_taxi.external_table_homework3
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-arnaudn/green_tripdata_2022-*.parquet']
);

CREATE TABLE ny_taxi.homework3_bigquery
AS
SELECT *
FROM `crypto-iris-412017.ny_taxi.external_table_homework3`;

SELECT count(*) FROM ny_taxi.homework3_bigquery;

SELECT DISTINCT PULocationID FROM
ny_taxi.external_table_homework3;

SELECT DISTINCT PULocationID FROM
ny_taxi.homework3_bigquery;

SELECT count(1) FROM ny_taxi.external_table_homework3
WHERE fare_amount = 0;

CREATE TABLE ny_taxi.homework3_cluster_partition
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS
SELECT *
FROM `crypto-iris-412017.ny_taxi.external_table_homework3`;

SELECT DISTINCT PULocationID 
FROM ny_taxi.homework3_bigquery
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';

SELECT DISTINCT PULocationID 
FROM ny_taxi.homework3_cluster_partition
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';

SELECT count(*) FROM ny_taxi.external_table_homework3;






