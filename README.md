# Zoomcamp2024

## To setup docker + postgres

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

### To run [pgadmin](https://hub.docker.com/r/dpage/pgadmin4/) in docker use the following command

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
