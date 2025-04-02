"""This module contains helper functions for the main script."""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import fastf1 as ff1
import mappings

# functions
def convert_time(data, column, fillna_value=None):
    """convert the time column to seconds and set the first driver to 0 and NaN values to 200 seconds
    the input should look like this: "0 days 01:54:21.964000"  "0 days 00:00:10.933000"
    """
    # Convert to timedelta, then to float seconds
    data[column] = pd.to_timedelta(data[column]).dt.total_seconds()
    # set NaN values to 200 seconds (retired drivers)
    data[column] = data[column].fillna(fillna_value) if fillna_value is not None else data[column]
    return data

def get_session_results(session):
    """final results for the session

    :param object session: session object from fastf1 
    :return pd.DataFrame: final results of the session
    """
    session_results = session.results[['Abbreviation', 'TeamId', 'CountryCode', 'ClassifiedPosition', 'GridPosition', 'Time', 'Points']].copy()
    # adding a new column called retired and fill it with 0
    session_results['Retired'] = 0
    # if ClassifiedPosition is 'R' then the driver retired
    session_results.loc[session_results['ClassifiedPosition'] == 'R', 'Retired'] = 1
    # put ClassifiedPosition to 20 if the driver retired
    session_results.loc[session_results['ClassifiedPosition'] == 'R', 'ClassifiedPosition'] = 20
    # convert Time column to timedelta with only seconds difference from the first driver
    session_results = convert_time(session_results, 'Time', 200)
    # set the first and biggest number which is the first driver in this column to 0 because only the first driver has 1 hour and 50 minutes
    # and the others are relative to this driver - so the first dirver is 6861 seconds for instance and the second driver is 2.0
    session_results.at[session_results.index[0], 'Time'] = 0.0
    # get country name and to the data
    session_results['CountryCode'] = session.event.Country
    # get Location
    session_results['Location'] = session.event.Location
    # rename some columns
    session_results.columns = ['Driver', 'TeamId', 'Country', 'FinalPosition', 'GridPosition', 'RaceTimeDiff', 'Points', 'Retired', 'Location']

    return session_results

def get_session_laps(session):
    """session laps data include all the laps of all drivers. 
    each lap is a row in the dataframe.

    :param object session: session object from fastf1 
    :return pd.DataFrame: session laps data
    """

    session_laps = session.laps[['LapNumber', 'Driver', 'LapTime', 'Compound', 'TyreLife', 'Stint', 
                                 'FreshTyre', 'Team', 'TrackStatus', 'Position', 'Sector1Time', 'Sector2Time', 
                                 'Sector3Time', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SpeedST', 'LapStartTime']].copy()

    # convert LapTime column to seconds and set NaT values as 0
    session_laps = convert_time(session_laps, 'LapTime', 0)
    # doing this also for the sector times
    session_laps = convert_time(session_laps, 'Sector1Time', 0)
    session_laps = convert_time(session_laps, 'Sector2Time', 0)
    session_laps = convert_time(session_laps, 'Sector3Time', 0)
    session_laps = convert_time(session_laps, 'LapStartTime', 0)
    # set Speed trap columns to 0 if they are NaN
    session_laps['SpeedFL'] = session_laps['SpeedFL'].fillna(0)
    session_laps['SpeedST'] = session_laps['SpeedST'].fillna(0)
    session_laps['SpeedI1'] = session_laps['SpeedST'].fillna(0)
    session_laps['SpeedI2'] = session_laps['SpeedST'].fillna(0)
    # set Position as 20 if it is NaN (retired drivers)
    session_laps['Position'] = session_laps['Position'].fillna(-1)
    # convert LapStartTime name to SessionTime
    session_laps.rename(columns={'LapStartTime': 'SessionTime'}, inplace=True)
    # fresh tyre column to 0 and 1
    session_laps['FreshTyre'] = session_laps['FreshTyre'].apply(lambda x: 1 if x else 0)
    # map the compound names to the compound id
    session_laps['Compound'] = session_laps['Compound'].map(mappings.compounds)
    # map the team names to the team id
    session_laps['Team'] = session_laps['Team'].map(mappings.teams)
    # map driver names to the driver id
    session_laps['Driver'] = session_laps['Driver'].map(mappings.drivers)
    # sort values
    session_laps.sort_values("SessionTime", inplace=True)

    return session_laps


def get_weather_data(session):
    """get the weather data for the session

    :param object session: session object from fastf1 
    :return pd.DataFrame: weather data for the session
    """
    session_weather = session.weather_data.copy()
    # convert Time to seconds
    session_weather = convert_time(session_weather, 'Time')
    # convert Rainfall to 0 and 1
    session_weather['Rainfall'] = session_weather['Rainfall'].apply(lambda x: 1 if x else 0)
    # convert the name of Time in column to SessionTime
    session_weather.rename(columns={'Time': 'SessionTime'}, inplace=True)
    # sort values
    session_weather.sort_values("SessionTime", inplace=True)

    return session_weather

def aggregate_lap(lap_telemetry):
    """Generate telemetry statistics for a given lap.

    :param pd.DataFrame lap_telemetry: all telemetry data for a lap recorded every couple of milliseconds
    :return dict: lap statistics
    """
    result = {}
    # Check if lap_telemetry is empty
    if lap_telemetry.empty:
        result.update({
            "RpmAvg": None, "RpmMin": None, "RpmMax": None,
            "SpeedAvg": None, "SpeedMedian": None, "SpeedMin": None, "SpeedMax": None,
            "ThrottleAvg": None, "ThrottleMin": None, "ThrottleMax": None,
            "nGearAvg": None, "nGearMin": None, "nGearMax": None,
            "BrakeCount": 0, "DrsCount": 0, "nGearMode": None
        })
        return result

    # RPM statistics
    result["RpmAvg"] = lap_telemetry["RPM"].mean()
    result["RpmMin"] = lap_telemetry["RPM"].min()
    result["RpmMax"] = lap_telemetry["RPM"].max()

    # Speed statistics
    result["SpeedAvg"] = lap_telemetry["Speed"].mean()
    result["SpeedMedian"] = lap_telemetry["Speed"].median()
    result["SpeedMin"] = lap_telemetry["Speed"].min()
    result["SpeedMax"] = lap_telemetry["Speed"].max()

    # Throttle statistics
    result["ThrottleAvg"] = lap_telemetry["Throttle"].mean()
    result["ThrottleMin"] = lap_telemetry["Throttle"].min()
    result["ThrottleMax"] = lap_telemetry["Throttle"].max()

    # nGear statistics
    result["nGearAvg"] = lap_telemetry["nGear"].mean()
    result["nGearMin"] = lap_telemetry["nGear"].min()
    result["nGearMax"] = lap_telemetry["nGear"].max()

    # Brake and DRS counts (assuming a value > 0 indicates activation)
    result["BrakeCount"] = (lap_telemetry["Brake"] > 0).sum()
    result["DrsCount"] = (lap_telemetry["DRS"] > 0).sum()

    # Mode for nGear
    modes = lap_telemetry["nGear"].mode()
    result["nGearMode"] = modes.iloc[0] if not modes.empty else None

    return result


def calculate_lap_agg_telemetry(driver_laps, driver_car):
    """Aggregate telemetry data for each lap of a driver and merge it with the lap data. 

    :param pd.DataFrame driver_laps: all laps of a driver 
    :param pd.DataFrame driver_car: telemetry data for the session
    :return pd.DataFrame: laps data with telemetry statistics
    """
    # sort by session time
    driver_laps.sort_values("SessionTime", inplace=True)
    # Prepare a list to store aggregated results for each lap.
    lap_aggregates = []
    # We will treat the start of the session as time 0.
    prev_time = driver_laps["SessionTime"].min()
    # Iterate over each lap (using the SessionTime as the lap boundary)
    for idx, row in driver_laps.iterrows():
        lap_end = row["SessionTime"] + row["LapTime"]
        # Select telemetry data for the current lap interval:
        lap_telemetry = driver_car[(driver_car["SessionTime"] > prev_time) & (driver_car["SessionTime"] <= lap_end)]
        
        # Aggregate telemetry for this lap
        agg_result = aggregate_lap(lap_telemetry)
        # set row number as lap number
        agg_result["LapNumber"] = row["LapNumber"] if "LapNumber" in row else idx
        # Add the aggregated result for this lap.
        lap_aggregates.append(agg_result)
        # Update the previous lap boundary to the current lap's SessionTime.
        prev_time = lap_end

    # Convert the list of dictionaries to a DataFrame.
    driver_summary = pd.DataFrame(lap_aggregates)
    # join this dataframe to driver_laps dataframe
    driver_laps = pd.merge(driver_laps, driver_summary, on="LapNumber", how="left")
    return driver_laps


def calculate_session_laps_final(session:object, session_laps:pd.DataFrame):
    # selecting the drivers
    drivers=session.laps['Driver'].unique()
    print("Drivers in this session: ")
    print(drivers , "\n")
    # define an empty dataframe to store the final results
    session_laps_final = pd.DataFrame()
    # loop through the drivers to create a summary statistic for each telemetry data
    for drv in drivers:
        try:
            # get car data for the driver
            driver_car = session.laps.pick_drivers(drv).get_car_data()
        except KeyError:
            print(f"Driver {drv} not available")
            continue
        # convert Time into milliseconds
        driver_car = convert_time(driver_car, 'Time')
        driver_car = convert_time(driver_car, 'SessionTime')
        # sort by SessionTime
        driver_car.sort_values("SessionTime", inplace=True)
        # drop Source and Date columns
        driver_car.drop(columns=['Source', 'Date'], inplace=True)
        # rename some columns
        driver_car.columns = ['RPM', 'Speed', 'nGear', 'Throttle' , 'Brake', 'DRS', 'DrivingTime', 'SessionTime']
        # convert brake into 0 and 1
        driver_car['Brake'] = driver_car['Brake'].apply(lambda x: 1 if x else 0)
        # select laps for the driver from laps data
        driver_laps = session_laps[session_laps['Driver'] == mappings.drivers[drv]].copy()
        # calculate lap statistics for each driver
        driver_laps = calculate_lap_agg_telemetry(driver_laps, driver_car)
        # add the driver laps to the session laps
        session_laps_final = pd.concat([session_laps_final, driver_laps], ignore_index=True) if not session_laps_final.empty else driver_laps.copy()

    return session_laps_final