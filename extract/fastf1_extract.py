import sys
from pathlib import Path

import pandas as pd
from pandas import DataFrame

import fastf1
from fastf1.core import Laps, Session, SessionResults, Telemetry
from fastf1.events import EventSchedule
from fastf1.core import DataNotLoadedError

YEAR = sys.argv[1]

fastf1.Cache.enable_cache("fastf1_cache/")

def main():
    """
    Get the event schedule

    For each event schedule qualifying and major race event get:
        - driver_info_and results
        - lap_data
        - car_telemetry_data
        - car_position_data
        - weather_data
        - race_control_messages

    Save according to this file structure:
        2018/
            event_schedule.csv
            schedule/
                Bahrain_Grand_Prix/
                    Qualifying/
                        driver_info_and_results.csv
                        lap_data.csv
                        car_telemetry_data.csv
                        car_position_data.csv
                        weather_data.csv
                        race_control_messages.csv
                    Race/
                        driver_info_and_results.csv
                        lap_data.csv
                        car_telemetry_data.csv
                        car_position_data.csv
                        weather_data.csv
                        race_control_messages.csv
                Abu-Dhabi_Grand_Prix/
                ...
        2019/
        ...
    """

    print("Getting schedule data...")
    schedule = get_event_schedule(YEAR)
    path_sc = Path(f"data/{YEAR}/")
    path_sc.mkdir(parents=True, exist_ok=True)
    print("Writing schedule data...\n")
    schedule.to_csv(path_sc / "event_schedule.csv", index=False)

    for _, sc in schedule.iterrows():
        # Qualifying
        print(f"Getting session for {sc['EventName']} Qualifying...")
        if sc["EventFormat"] == "conventional":
            session_q = fastf1.get_session(YEAR, sc["RoundNumber"], "Q")
        else:  # event is of format "sprint"
            session_q = fastf1.get_session(YEAR, sc["RoundNumber"], "S")
        print("Loading session...")
        session_q.load()
        print("Getting all statistics...")
        all_statistics_q = get_all_statistics(session_q)

        path_q = Path(f"data/{YEAR}/schedule/{sc['EventName'].strip().replace(' ', '_')}/Qualifying/")
        path_q.mkdir(parents=True, exist_ok=True)
        print("Writing all statistics...\n")
        write_statistics(all_statistics_q, path_q)

        # Race
        print(f"Getting session for {sc['EventName']} Race...")
        session_r = fastf1.get_session(YEAR, sc["RoundNumber"], "R")
        print("Loading session...")
        session_r.load()
        print("Getting all statistics...")
        all_statistics_r = get_all_statistics(session_r)

        path_r = Path(f"data/{YEAR}/schedule/{sc['EventName'].strip().replace(' ', '_')}/Race/")
        path_r.mkdir(parents=True, exist_ok=True)
        print("Writing all statistics...\n")
        write_statistics(all_statistics_r, path_r)

        print(f"Done with {sc['EventName']}...\n\n")

def get_event_schedule(year:int) -> EventSchedule:
    """Gets event schedule data for a particuler year.
       Renames and returns specified columns
    """

    schedule = fastf1.get_event_schedule(year, include_testing=False)

    schedule_renamed = schedule.rename(
        mapper={"Session4Date": "QualifyingRaceDate", "Session5Date": "FinalRaceDate"},
        axis=1,
    )

    considered_columns = [
        "RoundNumber", "Country", "Location", "OfficialEventName",
        "EventName", "EventFormat",
        "QualifyingRaceDate", "FinalRaceDate"
    ]
    schedule_fin = schedule_renamed.loc[:, considered_columns]

    return schedule_fin

def get_all_statistics(session:Session) -> dict[str, pd.DataFrame]:
    """Returns all desired statistics for a particular event session"""
    
    return dict(
        driver_info_and_results_data=get_driver_info_and_results(session),
        lap_data=get_lap_data(session),
        car_data=get_car_data(session),
        car_position_data=get_car_position_data(session),
        weather_data=get_weather_data(session),
        rcm_data=get_race_control_messages(session),
    )

def get_driver_info_and_results(session:Session) -> "SessionResults|None":
    """Returns session results. This also contains complete driver information.
       Returns None if data is not loaded.
    """
    
    try:
        return session.results
    except DataNotLoadedError:
        # logging
        return None

def get_lap_data(session:Session) -> "Laps|None":
    """Returns lap data or None if lap data is not loaded."""

    try:
        return session.laps
    except DataNotLoadedError:
        # logging
        return None

def get_car_data(session:Session) -> "DataFrame|None":
    """Returns car data for all drivers or None if not loaded"""

    try:
        car_dict:dict[str, Telemetry] = session.car_data

        car_list:list[Telemetry] = [
            car_data.assign(DriverNumber=driver_num)
            for driver_num, car_data in car_dict.items()
        ]
        return pd.concat(car_list)
    except DataNotLoadedError:
        # logging
        return None

def get_car_position_data(session:Session) -> "DataFrame|None":
    """Returns car position data or None if not loaded"""

    try:
        car_position_dict:dict[str, Telemetry] = session.pos_data

        car_position_list:list[Telemetry] = [
            car_data.assign(DriverNum=driver_num)
            for driver_num, car_data in car_position_dict.items()
        ]
        return pd.concat(car_position_list)
    except DataNotLoadedError:
        # logging
        return None

def get_weather_data(session:Session) -> "DataFrame|None":
    """Returns weather data or None if it is not loaded"""
    
    try:
        return session.weather_data
    except DataNotLoadedError:
        # logging
        return None

def get_race_control_messages(session:Session) -> "DataFrame|dict":
    """Returns race control messages data or an empty dict if not loaded"""

    return session.race_control_messages

def write_statistics(stats_dict:dict[str, pd.DataFrame], path:Path):
    """Write all statistics to disk"""

    for name, stats in stats_dict.items():
        if issubclass(type(stats), DataFrame):
            stats.to_csv(path / f"{name}.csv", index=False)
        else:
            # logging
            ...
