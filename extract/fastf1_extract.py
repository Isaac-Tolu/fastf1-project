import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

import fastf1
from fastf1.core import Lap, Laps, Session, Telemetry
from fastf1.events import EventSchedule
from fastf1.core import DataNotLoadedError

from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import create_engine, text

register_adapter(np.int64, AsIs)

YEAR, USER, PASSWORD = sys.argv[1:]
CONN_STR = f"postgresql+psycopg2://{USER}:{PASSWORD}@postgres/fastf1"

YEAR = int(YEAR)

fastf1.Cache.enable_cache("fastf1_cache/")

def main():
    
    try:
        engine = create_engine(CONN_STR)
        conn = engine.connect()

        session_info = get_session_info(YEAR)
        session_info.to_sql(name="dim_sessions", index=False, con=conn, if_exists='append')

        for _, sc in session_info.iterrows():
            session = fastf1.get_session(YEAR, sc["roundnumber"], sc["sessiontype"])

            session.load()

            new_drivers = get_new_drivers(session, conn)
            results_info = get_results(session, sc["sessionid"])

            if new_drivers is not None:
                new_drivers.to_sql(name="dim_drivers", index=False, con=conn, if_exists='append')
            if results_info is not None:
                results_info.to_sql(name="fact_results", index=False, con=conn, if_exists='append')

            lap_with_weather_info = get_lap_with_weather_info(session, sc["sessionid"])
            if lap_with_weather_info is not None:
                lap_with_weather_info.to_sql(name="fact_lap_statistics", index=False, con=engine, if_exists="append")

            lap_gen = session.laps.iterlaps()
            for lap in lap_gen:
                telemetry_data = get_telemetry_of_lap(lap[1], sc["sessionid"])
                if telemetry_data is not None:
                    telemetry_data.to_sql(name="fact_lap_telemetry_statistics", index=False, con=engine, if_exists="append")
                else:
                    break

    finally:
        conn.close()


def get_session_info(year:int) -> EventSchedule:
    """Gets event session data for a particular year.
    """

    schedule = fastf1.get_event_schedule(year, include_testing=False)

    mapper_q = {"Session4": "SessionType", "Session4Date": "SessionDate"}
    mapper_r = {"Session5": "SessionType", "Session5Date": "SessionDate"}

    schedule_q = schedule.rename(mapper=mapper_q, axis=1)
    schedule_r = schedule.rename(mapper=mapper_r, axis=1)

    schedule_all = pd.concat([schedule_q, schedule_r]).assign(Year=year).reset_index(drop=True)
    schedule_all["SessionID"] = schedule_all["Year"].astype('str') + "_"  \
        + schedule_all["RoundNumber"].astype('str') + "_" \
        + schedule_all["SessionType"].apply(lambda x: x[0])

    considered_columns = [
        "SessionID", "Year", "RoundNumber", "Country", "Location",
        "OfficialEventName", "EventName", "SessionType", "SessionDate"
    ]
    schedule_fin = schedule_all.loc[:, considered_columns]
    schedule_fin.columns = schedule_fin.columns.str.lower()

    return schedule_fin

def get_new_drivers(session:Session, conn) -> DataFrame:
    """Gets the newest drivers not yet added to database"""
    
    try:
        results = session.results.reset_index(names="DriverID")

        driver_info_cols = [
            "DriverID", "DriverNumber", "BroadcastName", "Abbreviation", "TeamName",
            "FirstName", "LastName", "FullName"
        ]
        driver_info = results.loc[:, driver_info_cols]
        driver_info.columns = driver_info.columns.str.lower()

        driver_info["driverid"] = driver_info["driverid"] + "_" + driver_info["teamname"]
        driver_info["drivernumber"] = driver_info["drivernumber"].astype("int")

        db_drivers = pd.read_sql(text("select * from dim_drivers"), con=conn)
        new_drivers = pd.concat([driver_info, db_drivers, db_drivers]).drop_duplicates(keep=False)

        return new_drivers
    except DataNotLoadedError:
        return None

def get_results(session:Session, session_id:str) -> DataFrame:
    """Gets results of the session race"""
    
    try:
        results = session.results

        results["DriverID"] = results["DriverNumber"].astype(str) + "_" + results["TeamName"]
        result_info_cols = [
            "DriverID", "Position", "GridPosition", "Q1", "Q2",
            "Q3", "Status", "Points"
        ]
        results_info  = results.loc[:, result_info_cols]
        results_info.columns = results_info.columns.str.lower()
        results_info = results_info.assign(sessionid=session_id)

        timedelta_cols = ["q1", "q2", "q3"]
        results_info = convert_td_cols_to_str(results_info, timedelta_cols)

        return results_info
    except DataNotLoadedError:
        return None

def get_lap_with_weather_info(session:Session, session_id:str) -> Laps:
    """Gets lap statistics data.
       Contains weather and other kinds of informaton about each lap"""

    try:
        lap_data = session.laps.reset_index(drop=True)
        weather_data = session.laps.get_weather_data().reset_index(drop=True)

        joined_data = pd.concat([lap_data, weather_data.loc[:, ~(weather_data.columns == 'Time')]], axis=1)

        joined_data = joined_data.assign(SessionID=session_id).rename({"DriverNumber": "DriverID"}, axis=1)

        joined_data["DriverID"] = joined_data["DriverID"] + '_' + joined_data["Team"] 
        joined_data["LapID"] = session_id + '_' + joined_data["DriverID"].astype("str") + '_' + joined_data["LapNumber"].astype(str)

        joined_data.columns = joined_data.columns.str.lower()
        time_delta_cols = [
            "time", "laptime", "pitouttime", "pitintime",
            "sector1time", "sector2time", "sector3time",
            "sector1sessiontime", "sector2sessiontime",
            "sector3sessiontime", "lapstarttime",
        ]
        joined_data = convert_td_cols_to_str(joined_data, time_delta_cols)
        joined_data.drop(['driver', 'team'], axis=1, inplace=True)

        return joined_data
    except DataNotLoadedError:
        return None

def get_telemetry_of_lap(lap:Lap, session_id: str) -> Telemetry:
    """Gets telemetry data for each lap in a particular session"""

    try:
        tel_data = lap.get_telemetry()
        tel_data["LapID"] = session_id + '_' + str(lap["DriverNumber"]) + '_'  + lap["Team"] + '_' + str(lap["LapNumber"])

        tel_data.columns = tel_data.columns.str.lower()

        timedelta_cols = ["sessiontime", "time"]
        tel_data = convert_td_cols_to_str(tel_data, timedelta_cols)

        return tel_data
    except DataNotLoadedError:
        return None

def convert_td_cols_to_str(df: DataFrame, td_cols:list) -> DataFrame:
    """Utility function for converting timedelta column to string.
       Postgres cannot read pandas timedelta column"""

    str_func = lambda x: str(x) if not pd.isnull(x) else None
    for col in td_cols:
        df[col] = df[col].apply(str_func)

    return df

if __name__ == "__main__":
    main()