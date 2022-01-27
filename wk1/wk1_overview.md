## Docker and Postgres

Run postgres in docker and add some data to it. 

Use the offical docker repo for postgres. 

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

To access the database - can use a client call pgcli. Looks through a lot of versions ot find correct version. 

## PgCLI and pgAdmin

```bash
pip install pgcli
```

Can also install using brew? It works but get some errors. 

```bash
pgcli --help
pgcli -h localhost -p 5432 -U root -d ny_taxi
```

Create a docker network and run postgressql and the pgAdmin together. 

```bash
docker network create pg-network
```


```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v c:/Users/alexe/git/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
  ```

Run pgAdmin on localhost:8080 - remember the the Hostname will now be pg-database which can be seen as on the same network. 


```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```


## Creating ingest.py

```bash
jupyter nbconvert --to=script
``` 

- Clean it up and use argparse to pass parameters. Then create a docker image with it.

```bash
docker build -t taxi_ingest:v001 .
```

- Spin up the the containers with docker-compose.yaml 

```
docker-compose up -d
```

*may need to change the network in the Dockerfile depending on what is created*

```bash
URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

docker run -it \
  --network=2_docker_sql_default \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```

## SQL 


