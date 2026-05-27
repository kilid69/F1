"""This module contains helper functions for the main script."""

import os
import json
import pandas as pd
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

def get_session_results(session) -> pd.DataFrame:
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

def get_weather_data(session) -> pd.DataFrame:
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


def clean_telemetry(telemetry: pd.DataFrame) -> pd.DataFrame:
    """Clean raw FastF1 telemetry samples so they are ready for modelling.

    The raw frame has one row per sensor sample (many per second) and mixes
    numerical signals (RPM, Speed, ...), text durations, booleans, metadata
    and 3D coordinates. This function turns it into a purely numerical frame
    that downstream code (aggregation, scaling, the neural network) can read.

    What it does, in order:
        1. Keep only on-track samples (drops off-track rows).
        2. Drop columns that carry no predictive signal:
              - ``Date``           wall-clock timestamp
              - ``Source``         metadata about how the sample was captured
              - ``Status``         already used to filter; no info left
              - ``DriverAhead``    driver-id ahead, noisy at sample level
              - ``Time``           same as ``SessionTime`` shifted by a constant
                                    (``Time = SessionTime - first_sample_SessionTime``)
                                    so it carries no extra information
              - ``Distance``       cumulative meters since recording start
              - ``RelativeDistance``  same as Distance, just rescaled to [0,1]
                                    over the data slice; not "where on the lap"
                                    unless the slice is exactly one lap
              - ``X``, ``Y``, ``Z``  track-specific coordinates (don't generalise)
        3. Convert ``SessionTime`` from a FastF1 text duration
           (e.g. ``"0 days 01:54:21.964000"``) to float seconds. ``SessionTime``
           is kept because the lap-aggregation step uses it to match each
           telemetry sample to the lap it belongs to.
        4. Convert ``Brake`` from boolean True/False to int 1/0.
        5. Simplify ``DRS``: FastF1 codes are 0, 1, 2, 3 where 2 and 3 mean
           DRS is active. Collapse to 1/0.
        6. Clean ``Throttle``: the value 104 is FastF1's sentinel for
           "unavailable" (typically when the car is stationary). Clip it
           back to the 0-100 range.
        7. Fill missing ``DistanceToDriverAhead`` (NaN means no car ahead)
           with 9999, meaning "very far / no traffic".

    Expected input columns (FastF1 default 18):
        Date, SessionTime, DriverAhead, DistanceToDriverAhead, Time, RPM,
        Speed, nGear, Throttle, Brake, DRS, Source, Distance,
        RelativeDistance, Status, X, Y, Z

    Output columns (8, all numeric):
        SessionTime, DistanceToDriverAhead, RPM, Speed, nGear,
        Throttle, Brake, DRS

    :param pd.DataFrame telemetry: raw telemetry samples from
        ``session.car_data`` / ``laps.get_telemetry()``.
    :return pd.DataFrame: cleaned, numerical telemetry, index reset.
    """
    # work on a copy so the caller's DataFrame is not mutated
    df = telemetry.copy()

    # 1. keep only on-track samples
    df = df[df["Status"] == "OnTrack"]

    # 2. drop columns we don't need (see docstring for the reasoning)
    drop_cols = [
        "Date", "DriverAhead", "Source", "Status",
        "Time",
        "Distance", "RelativeDistance",
        "X", "Y", "Z",
    ]
    df = df.drop(columns=drop_cols)

    # 3. text duration -> float seconds
    df["SessionTime"] = pd.to_timedelta(df["SessionTime"]).dt.total_seconds()

    # 4. Brake: True/False -> 1/0
    df["Brake"] = df["Brake"].astype(int)

    # 5. DRS: codes 0/1 -> 0 (not active), codes 2/3 -> 1 (active)
    df["DRS"] = (df["DRS"] >= 2).astype(int)

    # 6. Throttle: 104 = "unavailable" sentinel; cap at 100
    df["Throttle"] = df["Throttle"].clip(upper=100)

    # 7. fill missing "distance to car ahead" with a large sentinel value
    df["DistanceToDriverAhead"] = df["DistanceToDriverAhead"].fillna(9999)

    return df.reset_index(drop=True)


def clean_laps(laps: pd.DataFrame) -> pd.DataFrame:
    """Clean the FastF1 lap table so each row is one usable per-lap feature vector.

    Each row of the input is one lap by one driver. This function removes
    unreliable laps, drops columns the model does not need, derives a
    pit-lap flag, and converts FastF1 text durations to float seconds.

    What it does, in order:
        1. Drop unreliable / fake laps:
              - ``IsAccurate == False``     timing is not trustworthy
              - ``Deleted == True``         deleted by stewards (track limits, etc.)
              - ``FastF1Generated == True`` synthesised, e.g. crash laps
        2. Derive ``IsPitLap`` = 1 if the car entered or exited the pits this lap.
        3. Drop columns that carry no signal or are redundant:
              - ``Time``                   same as ``LapStartTime + LapTime``
              - ``DriverNumber``           duplicate of ``Driver``
              - ``FreshTyre``              redundant with ``TyreLife``
              - ``LapStartDate``           wall-clock datetime
              - ``Sector1/2/3SessionTime`` redundant with sector times
              - ``PitInTime``, ``PitOutTime``  already captured in ``IsPitLap``
              - ``DeletedReason``, ``FastF1Generated``, ``IsAccurate``,
                ``Deleted``                only used to filter
        4. Convert timedelta columns to float seconds:
              ``LapTime``, ``Sector1Time``, ``Sector2Time``, ``Sector3Time``,
              ``LapStartTime``.
        5. Convert ``IsPersonalBest`` from bool to int 0/1.

    Output columns (~18, all model-ready or kept as merge keys):
        Driver, Team, Compound, TrackStatus,
        LapTime, Sector1Time, Sector2Time, Sector3Time,
        SpeedI1, SpeedI2, SpeedFL, SpeedST,
        LapNumber, Stint, TyreLife, Position, IsPersonalBest, IsPitLap,
        LapStartTime   (merge key with telemetry; drop after merging)

    :param pd.DataFrame laps: raw lap data from ``session.laps``.
    :return pd.DataFrame: cleaned lap table, index reset.
    """
    df = laps.copy()

    # 1. drop unreliable / fake laps
    df = df[
        (df["IsAccurate"].fillna(False)) &
        (~df["Deleted"].fillna(False)) &
        (~df["FastF1Generated"].fillna(False))
    ].copy()  # explicit copy so subsequent column assignments don't warn

    # 2. derive IsPitLap (1 if the car entered OR exited the pit during this lap)
    df["IsPitLap"] = (df["PitInTime"].notna() | df["PitOutTime"].notna()).astype(int)

    # 3. drop columns we don't need (see docstring for the reasoning)
    drop_cols = [
        "Time", "DriverNumber", "FreshTyre", "LapStartDate",
        "Sector1SessionTime", "Sector2SessionTime", "Sector3SessionTime",
        "PitInTime", "PitOutTime",
        "DeletedReason", "FastF1Generated", "IsAccurate", "Deleted",
    ]
    df = df.drop(columns=drop_cols, errors="ignore")

    # 4. text durations -> float seconds
    for col in ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time", "LapStartTime"]:
        df[col] = pd.to_timedelta(df[col]).dt.total_seconds()

    # 5. IsPersonalBest: True/False -> 1/0
    df["IsPersonalBest"] = df["IsPersonalBest"].astype(int)

    return df.reset_index(drop=True)


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


def merge_telemetry_into_laps(laps: pd.DataFrame, telemetry: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cleaned telemetry into per-lap statistics and merge into the lap table.

    For each lap in ``laps``, the function:
        1. Picks the telemetry samples that fall within that lap's time window
           (``LapStartTime`` ≤ sample.SessionTime < ``LapStartTime + LapTime``).
        2. Aggregates those samples using :func:`aggregate_lap`, producing
           per-lap stats (``RpmAvg/Min/Max``, ``SpeedMax``, ``ThrottleAvg``,
           ``BrakeCount``, ``DrsCount``, ``nGearMode``, ...).
        3. Joins those stats back onto the lap row, keyed by ``LapNumber``.

    Both inputs must be **for a single driver**, already cleaned by
    :func:`clean_laps` and :func:`clean_telemetry`. The caller is responsible
    for filtering by driver before calling this function.

    :param pd.DataFrame laps: cleaned lap data for one driver.  Must contain
        ``LapNumber``, ``LapStartTime``, ``LapTime`` (all in seconds).
    :param pd.DataFrame telemetry: cleaned telemetry samples for the same
        driver.  Must contain ``SessionTime`` (in seconds).
    :return pd.DataFrame: ``laps`` with aggregated telemetry columns merged in.
    """
    aggregated_rows = []
    for _, lap in laps.iterrows():
        lap_start = lap["LapStartTime"]
        lap_end = lap_start + lap["LapTime"]

        # telemetry samples that belong to this lap
        lap_samples = telemetry[
            (telemetry["SessionTime"] >= lap_start) &
            (telemetry["SessionTime"] <  lap_end)
        ]

        stats = aggregate_lap(lap_samples)
        stats["LapNumber"] = lap["LapNumber"]
        aggregated_rows.append(stats)

    agg_df = pd.DataFrame(aggregated_rows)

    # merge the aggregates back into the lap table on LapNumber
    return laps.merge(agg_df, on="LapNumber", how="left")


def build_session_laps(session) -> pd.DataFrame:
    """Build the per-lap feature table for one full session — the input
    backbone for the inner LSTM.

    For each driver in the session:
        1. Clean the lap data with :func:`clean_laps`.
        2. Clean the telemetry with :func:`clean_telemetry`.
        3. Aggregate telemetry into per-lap stats with
           :func:`merge_telemetry_into_laps`.

    Then attach weather data via a nearest-time merge on ``LapStartTime``,
    and concatenate all drivers into a single DataFrame.

    The result is one row per ``(driver, lap)`` containing lap features +
    aggregated telemetry + weather — ready as one step of the inner LSTM
    sequence in the next-race prediction model.

    :param object session: a loaded FastF1 session (after ``session.load()``).
    :return pd.DataFrame: session-wide per-lap feature table.
    """
    drivers = session.laps["Driver"].unique()
    weather = get_weather_data(session)  # SessionTime already in seconds

    per_driver_frames = []
    for drv in drivers:
        # pull raw laps + raw telemetry for this driver
        try:
            raw_laps = session.laps.pick_drivers(drv)
            raw_telemetry = raw_laps.get_telemetry()
        except (KeyError, ValueError) as e:
            print(f"  skipping driver {drv}: {e}")
            continue

        # clean both
        try:
            laps = clean_laps(raw_laps)
            telemetry = clean_telemetry(raw_telemetry)
        except KeyError as e:
            print(f"  skipping driver {drv}: missing column {e}")
            continue

        if laps.empty:
            print(f"  skipping driver {drv}: no usable laps after cleaning")
            continue

        # aggregate telemetry into per-lap stats and merge into the lap table
        merged = merge_telemetry_into_laps(laps, telemetry)
        per_driver_frames.append(merged)

    if not per_driver_frames:
        return pd.DataFrame()

    session_laps = pd.concat(per_driver_frames, ignore_index=True)

    # attach weather using a nearest-time merge on LapStartTime
    session_laps = session_laps.sort_values("LapStartTime").reset_index(drop=True)
    weather = weather.sort_values("SessionTime").reset_index(drop=True)
    session_laps = pd.merge_asof(
        session_laps,
        weather,
        left_on="LapStartTime",
        right_on="SessionTime",
        direction="nearest",
    )
    # merge_asof keeps both keys; SessionTime is redundant with LapStartTime
    session_laps = session_laps.drop(columns=["SessionTime"])
    return session_laps


# =============================================================================
# LEGACY — in-race prediction model
# -----------------------------------------------------------------------------
# Everything below was built for the OLD target ("predict THIS race's final
# position from in-progress laps"). The new next-race prediction pipeline
# (functions above) does NOT use any of these. Kept here because we plan to
# revisit the in-race model later.
#
# Notable mismatches with the new model:
#   - `add_static_info` drops Points/RaceTimeDiff/Retired as "leakage", but
#     for the new target those are legitimate past-race history features.
#   - `convert_to_diff` uses per-lap normalization that suits the old design.
# =============================================================================


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