## Week 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

### Question 1:

**Install Spark and PySpark**

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

Output is `res0: String = 3.5.1`

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

### Question 2

**FHV October 2019**

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB

```python
df.schema
types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropOff_datetime', types.TimestampType(), True),
    types.StructField('PUlocationID', types.IntegerType(), True),
    types.StructField('DOlocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)])

df = df.repartition(6)

df.write.parquet('fhv2019_repartitioned',mode="overwrite")
```

### Question 3

**Count records**

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 62,610

Code used to reach answer

```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime))\
    .withColumn('dropoff_date', F.to_date(df.dropOff_datetime))\
    .select('pickup_date', 'dropoff_date', 'PUlocationID', 'DOlocationID')\
    .filter("pickup_date='2019-10-15'")\
    .count()
```

> [!IMPORTANT]
> Be aware of columns order when defining schema


### Question 4

**Longest trip for each day**

What is the length of the longest trip in the dataset in hours?

- 631,152.50 Hours

Code used to reach answer

```python
df = df.withColumn('pickup_datetime', F.col('pickup_datetime').cast('timestamp')) \
       .withColumn('dropOff_datetime', F.col('dropOff_datetime').cast('timestamp')) \
       .withColumn('duration_in_hours', (F.col('dropOff_datetime').cast('long') - F.col('pickup_datetime').cast('long')) / 3600)

df = df.withColumn('pickup_date', F.to_date('pickup_datetime'))

result_df = df.groupBy('pickup_date') \
              .agg(F.max('duration_in_hours').alias('max_duration_in_hours')) \
              .orderBy(F.desc('max_duration_in_hours')) \
              .limit(5)

result_df.show()
```

### Question 5

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 4040



### Question 6

**Least frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- Jamaica Bay

Code used to reach answer

```python
df_zones = spark.read.parquet('zones')
df_zones.createOrReplaceTempView('zones')
df.createOrReplaceTempView('fhv2019_repartitioned')

spark.sql("""
SELECT
    pul.Zone,
    COUNT(1)
FROM
    fhv2019_repartitioned fhv LEFT JOIN zones pul ON fhv.PULocationID = pul.LocationID
GROUP BY
    1
ORDER BY
    2 ASC
LIMIT 5;
""").show()
```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw5
- Deadline: See the website