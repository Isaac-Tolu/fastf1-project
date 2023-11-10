# FastF1 Project
Extraction and modeling of F1 data

## Tools
- [FastF1](https://docs.fastf1.dev/) - A python library for accessing F1 data
- [Airflow](https://airflow.apache.org/) - A data orchestration tool
- [PostgreSQL](https://www.postgresql.org/) - A relational database

## Recreating the project
- Go to the airflow directory:
    ```
    cd airflow/
    ```
- If you have run airflow in docker before, run these commands first to start afresh:
    ```
    docker-compose down
    docker volume rm airflow_postgres-db-volume
    rm -r logs
    ```
- Run these commands:
    ```
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    mkdir logs/
    mkdir ../fastf1_cache/
    docker-compose build
    docker-compose up airflow-init
    docker-compose up -d
    ```
- Check periodically through `docker ps` whether the web server is healthy.
- Once the web server is healthy, go to `localhost:8080` on your browser. Use `airflow` as username and password.
- Trigger the DAG on the airflow UI.
- To check results on the database as the dag is running, use `psql` or any Postgres client you have to log in to Postgres. The database is available on port `5432`. The username is `airflow` and the database name is `fastf1`:
    ```
    psql -h localhost -p 5434 -U airflow -d fastf1
    ```
- If you can't access data from the terminal, go into the docker container and access Postgres there:
    ```
    docker exec -it <container_id> bash

    psql -U airflow -d fastf1
    ```
- You can run these queries on the database after the year 2019 has finished running on the Airflow UI to test that the data entered and everything is working perfectly:
    ```sql
    select * from dim_sessions;
    select * from dim_drivers;
    select * from fact_results;
    select * from fact_lap_statistics;
    select * from fact_lap_telemetry_statistics;
    ```
- For a rerun of years that stop unexpectedly:
    - Login into docker on the shell:
    ```
    docker exec -it <container_id> bash
    ```
    - For 2019:
    ```
    airflow dags backfill -s 2019-01-01 -e 2019-12-31 --reset-dagruns fastf1_extract
    ```
    - For 2020:
    ```
    airflow dags backfill -s 2020-01-01 -e 2020-12-31 --reset-dagruns fastf1_extract
    ``` 
    - For 2021:
    ```
    airflow dags backfill -s 2021-01-01 -e 2021-12-31 --reset-dagruns fastf1_extract
    ```
    - For 2022:
    ```
    airflow dags backfill -s 2022-01-01 -e 2022-12-31 --reset-0dagruns fastf1_extract
    ```
    - It has already been set not to repeat what has been run before.

## Project flow
- When running the containers initially, the database is created through the Postgres entry point. This [file](./airflow/pg_init_scripts/multiple_db.sh) runs before any container starts running
- When the dag is initialized, its sole job is to run this python [file](./extract/fastf1_extract.py) from 2019 to 2022.
- Python gets the event schedule for each year and structures it according to the database schema.
- For each session, the results and lap statistics are gotten and also sent to the database.

## Database Structure
- Fastf1 library data is sent to the database following the star schema
    ![schema](./artifacts/schema.png)
- The database contains 2 dimension tables: `dim_sessions` and `dim_drivers` that contain information about sessions and drivers
- There are 2 main fact tables: `fact_results` and `fact_lap_statistics` which contain race results and drivers lap statistics respectively
- `fact_lap_statistics` is further normalized into another table: `fact_lap_telemetry_statistics` which contains telemetry information for each lap.
- I should note here that in an ideal star schema, there should be only one fact table, but due to the data's complexity, normalizing the data's facts made it easier to reason about.
