## Data Structure
- Check the data here
- It is structured in this manner:
    ```
    2018/
        - schedule.csv
        schedule/
            Australian_Grand_Prix/
                Qualifying/
                    - car_data.csv
                    - car_position_data.csv
                    - driver_info_and_results_data.csv
                    - lap_data.csv
                    - rcm_data.csv
                    - weather_data.csv
                Race/
                    - car_data.csv
                    - car_position_data.csv
                    - driver_info_and_results_data.csv
                    - lap_data.csv
                    - rcm_data.csv
                    - weather_data.csv
            Austrian_Grand_Prix/
            ...
    2019/
    ...
    ```
- CSV files and what they contain:
    - schedule.csv: Schedule for the season. It contains the different events and the time for the Qualifying and Race sessions.
    - car_data.csv: Contains car telemetry data for each driver
    - car_position.csv: Contains car position data for each driver
    - driver_info_and_results_data.csv: Contains result of the event. It also contains complete driver information
    - lap_data.csv: Conatins full data about the different laps for each driver
    - rcm_data.csv: Contains data about race control messages
    - weather_data.csv: Contains data about the weather for that event

## Project Flow
- Check the extract python file here
- It contains a main function through which all other functions are accessed
- Here is the main flow:
    - The event schedule data for the year is first gotten and saved to disk
    - We iterate over each event in the event schedule
    - For each event, the qualifying and race sessions are gotten
    - Statistics for the qualifying session is gotten and saved to disk
    - Statistics for the race session is gotten and saved to disk


## Recreating the project
- Go to the airflow directory
    ```
    cd airflow/
    ```
- Run these commands
    ```
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    mkdir logs/
    docker-compose build
    docker-compose up airflow-init
    docker-compose up
    ```
- Check periodically through `docker ps` whether the webserver is healthy
- Once it is, go to `localhost:8080` on your webbrowser. Use `airflow` as username and password
- Trigger the DAG on the airflow UI