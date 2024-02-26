-- MAKE SURE YOU REPLACE taxi-rides-ny-339813-412521 WITH THE NAME OF YOUR DATASET! 
-- When you run the query, only run 5 of the ALTER TABLE statements at one time (by
-- highlighting only 5).
-- Otherwise BigQuery will say too many alterations to the table are being made.
CREATE TABLE  `earnest-cosmos-412021.trips_data_all.green_trip_data` as
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`; 


CREATE TABLE  `earnest-cosmos-412021.trips_data_all.yellow_trip_data` as
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`;

insert into  `earnest-cosmos-412021.trips_data_all.green_trip_data` 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020` ;


insert into  `earnest-cosmos-412021.trips_data_all.yellow_trip_data` 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`; 

-- Fixes yellow table schema
ALTER TABLE `earnest-cosmos-412021.trips_data_all.yellow_trip_data`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.yellow_trip_data`
  RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.yellow_trip_data`
  RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.yellow_trip_data`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.yellow_trip_data`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.yellow_trip_data`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.yellow_trip_data`
  RENAME COLUMN dropoff_location_id TO DOLocationID;

-- Fixes green table schema
ALTER TABLE `earnest-cosmos-412021.trips_data_all.green_trip_data`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.green_trip_data`
  RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.green_trip_data`
  RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.green_trip_data`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.green_trip_data`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.green_trip_data`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `earnest-cosmos-412021.trips_data_all.green_trip_data`
  RENAME COLUMN dropoff_location_id TO DOLocationID;
