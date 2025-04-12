"""This module contains helper functions for the main script."""

import os
import json
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
    # put ClassifiedPosition to 20 if the driver has DNF
    session_results.loc[session_results['ClassifiedPosition'] == 'D', 'ClassifiedPosition'] = 20
    # convert Time column to timedelta with only seconds difference from the first driver
    session_results = convert_time(session_results, 'Time', 200)
    # set the first and biggest number which is the first driver in this column to 0 because only the first driver has 1 hour and 50 minutes
    # and the others are relative to this driver - so the first dirver is 6861 seconds for instance and the second driver is 2.0
    session_results.at[session_results.index[0], 'Time'] = 0.0
    # get country name and to the data
    session_results['CountryCode'] = session.event.Country
    # get Location
    session_results['Location'] = session.event.Location
    # get year
    session_results['Year'] = session.date.year
    # rename some columns
    session_results.columns = ['Driver', 'TeamId', 'Country', 'FinalPosition', 'GridPosition', 'RaceTimeDiff', 'Points', 'Retired', 'Location', 'Year']

    # map the track name to the track id
    session_results['Track'] = session_results['Location'].map(mappings.tracks)

    return session_results

def get_session_laps(session):
    """session laps data include all the laps of all drivers. 
    each lap is a row in the dataframe.

    :param object session: session object from fastf1 
    :return pd.DataFrame: session laps data
    """

    session_laps = session.laps[['LapNumber', 'Driver', 'LapTime', 'Compound', 'TyreLife', 'Stint', 'PitInTime', 'PitOutTime',
                                 'FreshTyre', 'Team', 'TrackStatus', 'Position', 'Sector1Time', 'Sector2Time', 
                                 'Sector3Time', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SpeedST', 'LapStartTime']].copy()

    # convert LapTime column to seconds and set NaT values as 0
    session_laps = convert_time(session_laps, 'LapTime', 0)
    # doing this also for the sector times
    session_laps = convert_time(session_laps, 'Sector1Time', 0)
    session_laps = convert_time(session_laps, 'Sector2Time', 0)
    session_laps = convert_time(session_laps, 'Sector3Time', 0)
    session_laps = convert_time(session_laps, 'LapStartTime', 0)
    # again for the pit in and out times
    session_laps = convert_time(session_laps, 'PitInTime', 0)
    session_laps = convert_time(session_laps, 'PitOutTime', 0)
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
    # fill empty strings in TrackStatus column with 0
    session_laps['TrackStatus'] = session_laps['TrackStatus'].replace('', np.nan)
    # convert TrackStatus column to float
    session_laps['TrackStatus'] = session_laps['TrackStatus'].astype(float)
    # map track status to the track status id
    session_laps['TrackStatus'] = session_laps['TrackStatus'].map(mappings.track_status)

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

def add_static_info(session_results:pd.DataFrame):
    # import drivers.json as dictionary
    
    with open('drivers.json') as f:
        drivers = json.load(f)
    
    # Add age of the driver, exprience and achievements to the session results
    for drv in session_results['Driver'].unique():
        # get the driver data from the json
        driver_info = next((driver for driver in drivers['drivers'] if driver['abbreviation'] == drv), None)

        if driver_info is None:
            continue
        
        session_results.loc[session_results['Driver'] == drv, 'Age'] = driver_info['age']
        session_results.loc[session_results['Driver'] == drv , 'Exprience'] = driver_info['GPs Entered']
        session_results.loc[session_results['Driver'] == drv , 'Achievements'] = driver_info['points']
        session_results.loc[session_results['Driver'] == drv , 'AchievementsByTime'] = driver_info['points'] / driver_info['GPs Entered']
        
    # map driver names to the driver id
    session_results['Driver'] = session_results['Driver'].map(mappings.drivers)
    
    # we have to drop points, RacetimeDif and Retired column because it will leak the result to the model
    session_results.drop(columns=['Points', 'RaceTimeDiff', 'Retired'], inplace=True)

    return session_results

def add_points(session_results:pd.DataFrame):
    """Add last year points to give the model a weight of recent achievements"""
    # create to new empty columns for points
    session_results['LastYearDriverPoints'] = 0
    session_results['LastYearTeamPoints'] = 0
    # get the year from the session results
    year = session_results['Year'].unique()[0]
    # map last year points
    for drv_id in session_results['Driver'].unique():
        # map driver points of last year
        session_results.loc[session_results['Driver'] == drv_id , 'LastYearDriverPoints'] = mappings.year_driver_points[year-1][drv_id] if drv_id in mappings.year_driver_points[year-1] else 0
    for team_id in session_results['Team'].unique():
        # map team points of last year
        session_results.loc[session_results['Team'] == team_id , 'LastYearTeamPoints'] = mappings.year_team_points[year-1][team_id] if team_id in mappings.year_team_points[year-1] else 0
    return session_results

def final_preprocessing(df):
    # drop the laps without any telemetry data
    df = df.dropna(subset=['RpmMin', 'SpeedMin'], how='all')
    # change FinalPosition = w to 0
    df.loc[df['FinalPosition'] == 'W', 'FinalPosition'] = 20
    # convert final position to int
    try:
        df['FinalPosition'] = df['FinalPosition'].astype(int)
    except ValueError:
        print("FinalPosition for practices are NaN.")
    # remove the races with more than 78 laps - probably caused by false data fetching
    # Step 1: Count number of rows (laps) per race
    lap_counts = df.groupby(['Year', 'Location', 'Driver']).size().reset_index(name='LapCount')
    # Step 2: Keep only races with 78 or fewer laps
    valid_races = lap_counts[lap_counts['LapCount'] <= 80]
    # Step 3: Filter the original dataframe to only include those races
    df = df.merge(valid_races[['Year', 'Location', 'Driver']], on=['Year', 'Location', 'Driver'], how='inner')
    # convert LapNumber to int
    df['LapNumber'] = df['LapNumber'].astype(int)
    # transform to difference instead of absolute
    df = convert_to_diff(df)
    return df

def convert_to_diff(df):
    df_new = df.copy()
    # convert two int columns to float
    df_new[['BrakeCount', 'DrsCount']] = df_new[['BrakeCount', 'DrsCount']].astype(float)
    # convert pit in time and pit out time to int
    df_new[['PitInTime', 'PitOutTime']] = df_new[['PitInTime', 'PitOutTime']].astype(int)
    # feature engineering
    years=df_new['Year'].unique()
    for year in years:
        print(f"Year processing is: {year}")
        year_df = df_new[df_new['Year'] == year].copy()
        # get Locations
        locations = year_df['Location'].unique()
        for i, loc in enumerate(locations):
            location_df = year_df[year_df['Location'] == loc].copy()
            if i % 10 == 0:
                print(f"Location processing is: {i} / {len(locations)}")

            # filter the location_df to the rows that doesn't have pit in and pit out times
            location_dff = location_df[(location_df['PitInTime'] == 0) & (location_df['PitOutTime'] == 0)]
            # get the average of car data for the lap for all drivers
            lap_avg = location_dff[['RpmAvg', 'RpmMin', 'RpmMax', 'SpeedAvg', 'SpeedMedian', 'SpeedMin', 'SpeedMax',
                                'ThrottleAvg','ThrottleMin', 'ThrottleMax', 'nGearAvg', 'nGearMin', 'nGearMax',
                                'BrakeCount', 'DrsCount','Sector1Time', 'Sector2Time', 'Sector3Time', 'SpeedI1', 
                                'SpeedI2', 'SpeedFL','SpeedST', 'SessionTime', 'LapTime']].mean()
            # compare all drivers to the average of the lap and add modify the columns in place
            for col in lap_avg.index:
                location_df[col] = location_df[col] - lap_avg[col]
                # add the lap_df to the df_new dataframe
                df_new.loc[(df_new['Year']==year)&(df_new['Location']==loc), col] = location_df[col]

    return df_new


def is_driver_active(df):
    df['IsDriverActive'] = 0
    df.loc[df['LastYearDriverPoints'] > 20, 'IsDriverActive'] = 1
    return df