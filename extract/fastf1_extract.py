import sys, logging, time

import numpy as np
import pandas as pd
from pandas import DataFrame

import fastf1
from fastf1.core import Lap, Laps, Session, Telemetry
from fastf1.events import EventSchedule
from fastf1.core import DataNotLoadedError

import sqlalchemy as sa
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64, AsIs)

YEAR, USER, PASSWORD = sys.argv[1:]
CONN_STR = f"postgresql+psycopg2://{USER}:{PASSWORD}@postgres/fastf1"

YEAR = int(YEAR)

fastf1.Cache.enable_cache("fastf1_cache/")

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
handler.setFormatter(format)
logger.addHandler(handler)

def main():
    engine = sa.create_engine(CONN_STR)

    session_info = get_session_info(YEAR)
    upsert_df(session_info, "dim_sessions", engine)
    logger.info(f"Upserted session information for year {YEAR}")

    latest_results = get_latest_results(engine)
    if len(latest_results) == 2:
        checkpoint = latest_results[1]
    else:
        checkpoint = latest_results[0]

    for _, sc in session_info.iterrows():
        round_number = sc["roundnumber"]
        session_id = sc["sessionid"]
        session_type = sc["sessiontype"]

        if (session_type == "Qualifying") and (checkpoint[0] == "Race"):
            logger.info(f"Session already ran: {session_id}")
            continue # All Qualifying has already been sent to database 
        elif (session_type == "Qualifying") and (session_id < checkpoint[1]):
            logger.info(f"Session already ran: {session_id}")
            continue # All Qualifying below the checkpoint has been sent to database
        elif (session_type == "Race") and (session_id < checkpoint[1]):
            logger.info(f"Session already ran: {session_id}")
            continue # All Race below the checkpoint has been sent to database


        session = fastf1.get_session(YEAR, round_number, session_type)

        session.load()
        
        driver_info = get_driver_info(session, session_id)
        if driver_info is not None:
            upsert_df(driver_info, "dim_drivers", engine)
            logger.info(f"Upserted drivers for session {session_id}")

        results_info = get_results(session, session_id)
        if results_info is not None:
            upsert_df(results_info, "fact_results", engine)
            logger.info(f"Upserted results for session {session_id}")

        lap_with_weather_info = get_lap_with_weather_info(session, session_id)
        if lap_with_weather_info is not None:
            upsert_df(lap_with_weather_info, "fact_lap_statistics", engine, match_columns=["sessionid", "driverid", "lapid"])
            logger.info(f"Upserted lap and weather statistics for session {session_id}")

            lap_gen = session.laps.iterlaps()
            for lap in lap_gen:
                car_data = get_car_data_of_lap(lap[1], session_id)
                pos_data = get_pos_data_of_lap(lap[1], session_id)

                if (car_data is None) or (pos_data is None):
                    logger.warning(f"No telemetry data for session {session_id}")
                    break
                else:
                    upsert_df(car_data[1], "fact_lap_car_statistics", engine, match_columns=["lapid", "date"])
                    logger.info(f"Upserted car statistics for lap {car_data[0]}")

                    upsert_df(pos_data[1], "fact_lap_position_statistics", engine, match_columns=["lapid", "date"])
                    logger.info(f"Upserted position statistics for lap {pos_data[0]}")

                time.sleep(3)

            logger.info(f"Upserted car and position statistics for session {session_id}")
            time.sleep(10)


def upsert_df(df, table_name, engine, match_columns=None):
    """
    Perform an "upsert on a PostgreSQL table from a pandas DataFrame.

    Constructs an INSERT ... ON CONFLICT statement, uploads the DataFrame to a 
    temporary table and then executes the INSERT

    Parameters
    ----------
    df: pandas.DataFrame
        The DataFrame to be upserted.
    table _name: str
        The name of the target table.
    engine: sqlalchemy.engine.Engine
        The SQLAlchemy engine to use
    match_columns: list[str], optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.  
    """
    
    table_spec = '"' + table_name + '"'

    df_columns = list(df.columns)
    if not match_columns:
        insp = sa.inspect(engine)
        match_columns = insp.get_pk_constraint(table_name, schema=None)[
            "constrained_columns"
        ]
    columns_to_update = [col for col in df_columns if col not in match_columns]
    insert_col_list = ", ".join([f'"{col_name}"' for col_name in df_columns])
    stmt = f"INSERT INTO {table_spec} ({insert_col_list})\n"
    stmt += f"SELECT {insert_col_list} FROM temp_table\n"
    match_col_list = ", ".join([f'"{col}"' for col in match_columns])
    stmt += f"ON CONFLICT ({match_col_list}) DO UPDATE SET\n"
    stmt += ", ".join(
        [f'"{col}" = EXCLUDED."{col}"' for col in columns_to_update]
    )

    with engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS temp_table")
        conn.exec_driver_sql(
            f"CREATE TEMPORARY TABLE temp_table AS SELECT * FROM {table_spec} WHERE false"
        )
        df.to_sql("temp_table", conn, if_exists="append", index=False)
        conn.exec_driver_sql(stmt)

def get_latest_results(engine):

    with engine.begin() as conn:
        stmt = """\
            select x.sessiontype, max(x.sessionid)
            from (
                select distinct d.sessionid, d.sessiontype
                from fact_lap_statistics f join dim_sessions d
                    on d.sessionid = f.sessionid
                where year = 2019) x
            group by x.sessiontype;
        """

        results = conn.exec_driver_sql(stmt)
    return sorted(results.fetchall())

def get_session_info(year:int):
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

def get_driver_info(session:Session, session_id:str):
    """Gets drivers for a session"""
    
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

        return driver_info
    except DataNotLoadedError:
        logger.warning(f"No driver data for session {session_id}")
        return None

def get_results(session:Session, session_id:str):
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
        logger.warning(f"No results data for session {session_id}")
        return None

def get_lap_with_weather_info(session:Session, session_id:str):
    """Gets lap statistics data.
       Contains weather and other kinds of informaton about each lap"""

    try:
        lap_data = session.laps.reset_index(drop=True)
        weather_data = session.laps.get_weather_data().reset_index(drop=True)

        joined_data = pd.concat([lap_data, weather_data.loc[:, ~(weather_data.columns == 'Time')]], axis=1)

        joined_data = joined_data.assign(SessionID=session_id).rename({"DriverNumber": "DriverID"}, axis=1)

        joined_data["DriverID"] = joined_data["DriverID"] + '_' + joined_data["Team"]
        joined_data["LapID"] = session_id + '_' + joined_data["DriverID"].astype("str") + '_' + joined_data["LapNumber"].astype(int).astype(str)
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
        logger.warning(f"No lap statistics info for session {session_id}")
        return None

def get_car_data_of_lap(lap:Lap, session_id:str):
    """Gets car data for a lap in a session"""

    try:
        car_data = lap.get_car_data()
        lap_id = session_id + '_' + str(lap["DriverNumber"]) + '_' + lap["Team"] + '_' + str(int(lap["LapNumber"]))
        car_data["LapID"] = lap_id
        car_data = car_data.loc[:, ~(car_data.columns == 'Source')]

        car_data.columns = car_data.columns.str.lower()

        td_cols = ["time", "sessiontime"]
        car_data = convert_td_cols_to_str(car_data, td_cols)

        return lap_id, car_data
    except DataNotLoadedError:
        logger.warning(f"No car data information for lap {lap_id}")
        return None
    
def get_pos_data_of_lap(lap:Lap, session_id:str):
    """Gets car position data for a lap in a session"""

    try:
        pos_data = lap.get_pos_data()
        lap_id = session_id + '_' + str(lap["DriverNumber"]) + '_' + lap["Team"] + '_' + str(int(lap["LapNumber"]))
        pos_data["LapID"] = lap_id
        pos_data = pos_data.loc[:, ~(pos_data.columns == 'Source')]

        pos_data.columns = pos_data.columns.str.lower()

        td_cols = ["time", "sessiontime"]
        pos_data = convert_td_cols_to_str(pos_data, td_cols)

        return lap_id, pos_data
    except DataNotLoadedError:
        logger.warning(f"No position data information for lap {lap_id}")
        return None

def convert_td_cols_to_str(df: DataFrame, td_cols:list):
    """Utility function for converting timedelta column to string.
       Postgres cannot read pandas timedelta column"""

    str_func = lambda x: str(x) if not pd.isnull(x) else None
    for col in td_cols:
        df[col] = df[col].apply(str_func)

    return df

if __name__ == "__main__":
    main()