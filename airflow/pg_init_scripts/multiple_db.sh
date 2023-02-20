#!/bin/bash

set -e
set -u

function create_user_and_database() {
	local database=$1
	echo "  Creating user and database '$database'"
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
	    CREATE USER $database;
	    CREATE DATABASE $database;
	    GRANT ALL PRIVILEGES ON DATABASE $database TO $database;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
	echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
	for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
		create_user_and_database $db
	done
	echo "Multiple databases created"
fi

echo "Creating fastf1 tables"
psql -U $POSTGRES_USER -d $POSTGRES_MULTIPLE_DATABASES <<-EOSQL
	create table dim_sessions (
  		SessionID text primary key,
  		Year int,
  		RoundNumber int,
  		Country text,
  		Location text,
  		OfficialEventName text,
  		EventName text,
  		SessionType text,
  		SessionDate date
	);

	create table dim_drivers (
		DriverID text primary key,
		DriverNumber int,
		BroadcastName text,
		Abbreviation text,
		TeamName text,
		FirstName text,
		LastName text,
		FullName text
	);

	create table fact_results (
		SessionID text,
		DriverID text,
		Position int,
		GridPosition float,
		Q1 interval,
		Q2 interval,
		Q3 interval,
		Status text,
		Time interval,
		Points float,
		foreign key (SessionID) references dim_sessions(SessionID),
		foreign key (DriverID) references dim_drivers(DriverID)
	);

	create table fact_lap_statistics (
		SessionID text,
		DriverID text,
		LapID text primary key,
		Time interval,
		LapTime interval,
		LapNumber int,
		PitOutTime interval,
		PitInTime  interval,
		Sector1Time interval,
		Sector2Time interval,
		Sector3Time interval,
		Sector1SessionTime interval,
		Sector2SessionTime interval,
		Sector3SessionTime interval,
		SpeedI1 float,
		SpeedI2 float,
		SpeedFL float,
		SpeedST float,
		IsPersonalBest boolean,
		Compound text,
		TyreLife float,
		FreshTyre text,
		Stint int,
		LapStartTime interval,
		TrackStatus int,
		IsAccurate boolean,
		LapStartDate timestamp,
		AirTemp float,
		Humidity float,
		Pressure float,
		Rainfall boolean,
		TrackTemp float,
		WindDirection int,
		WindSpeed float,
		foreign key (SessionID) references dim_sessions (SessionID),
		foreign key (DriverID) references dim_drivers (DriverID)
	);

	create table fact_lap_telemetry_statistics (
		LapID text,
		Date timestamp,
		SessionTime interval,
		DriverAhead text,
		DistanceToDriverAhead float,
		Time interval,
		RPM int,
		Speed int,
		nGear int,
		Throttle int,
		Brake boolean,
		DRS int,                    
		Source text,
		Distance float,
		RelativeDistance float,
		Status text,
		X int,
		Y int,
		Z int,                 
		foreign key (LapID) references fact_lap_statistics (LapID)
	);
EOSQL

echo "tables created."