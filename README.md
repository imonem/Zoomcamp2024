# Zoomcamp2024

## To setup docker + postgres

```bash
docker pull postgres
```

```bash
docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="ny_taxi" \
-v "./ny-taxi-volume:/var/lib/postgresql/data" \
-p 5432:5432 \
postgres:13
```

## To connect to docker image

```python
pip install pgcli
```

if not already installed

then type the below bash command

```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

## Get data from URLs below

### Parquet file

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
```

### Data dictionary

```bash
wget https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
```

### Parquqet files

```bash
wget https://www.nyc.gov/assets/tlc/downloads/pdf/working_parquet_format.pdf
```

## pgadmin docker image

```bash
docker pull dpage/pgadmin4
```

### To run [pgadmin](https://hub.docker.com/r/dpage/pgadmin4/) in docker, use the following command

```bash
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
dpage/pgadmin4
```

### Create a docker [network](https://docs.docker.com/engine/reference/commandline/network_create/) to connect postgres container and pgadmin container

```bash
docker network create pg-network
```

### Configuring the docker network to link postgres and pgadmin containers

```bash
docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="ny_taxi" \
-v "./ny-taxi-volume:/var/lib/postgresql/data" \
-p 5432:5432 \
--network=pg-network \
--name pg-database \
postgres:13
```

***Adjust the connection string from above because the name is important***

```bash
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network=pg-network \
--name pgadmin \
dpage/pgadmin4
```

***Use the following command to start exited container***

```bash
docker start pg-database && docker attach pg-database
```

```bash
docker start pgadmin && docker attach pgadmin
```

## Dockerizing the ingest process

### First step is to turn the jupyter notebook into a python script using the below command

```bash
jupyter nbconvert --to python ny_taxi_ingest
```

### Second step is to clean the output script and add a `__main__` block

```python
if __name__ = '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='hostname for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we will write the result to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)
```

### Third step is to test the data ingestion

```bash
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet"
```

```bash
python ny_taxi_ingest.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}
```

After successfully running the above, we test using pgadmin instance on browser
We run a count query such as the below

```sql
SELECT
	COUNT(1)
FROM
	yellow_taxi_data
```

which *assuming we used the above URL* returns ```449063```

We are now ready to build the Dockerfile

### Fourth step is creating the Docker build file

#### *Note that the file name must be Dockerfile*

```Dockerfile
FROM python:3.10.13

RUN apt-get install wget
RUN pip install pandas pyarrow fastparquet sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
```

then do a build command similar to the below

```bash
docker build -t taxi_ingest:v001 .
```

### Launching the new docker container image

We use the same paramters we used on the data ingestion script

```bash
docker run -it \
--network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pg-database \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_data \
        --url=${URL}
```

*Note that the second line has ```--network=pg-network``` because we need the container to run in the same network as the database*

```--host=localhost``` is changed to ```--host=pg-database``` which is the database instance.

The third line is the image name followed by the parameters

### Docker compose
