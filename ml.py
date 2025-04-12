import pandas as pd
import numpy as np
import fastf1 as ff1
import time
import helpers
from tqdm import tqdm
import logging
import os

# Suppress FastF1 logging messages
logging.getLogger('fastf1').setLevel(logging.WARNING)

# years
years = [2025]
# Race sessions
sessions = ['R']

for year in years:
    # empty dataframe to store the all seasons and all sessions data
    final_data = pd.DataFrame()

    for s in sessions:
        # the count of tracks in each year is not over 24
        for track in tqdm(range(3, 5)):
            
            try:
                session = ff1.get_session(year, track, s)
            except ValueError:
                print(f"Track {track} not available")
                break

            # Load the data
            session.load()
              
            session_results = helpers.get_session_results(session)

            session_laps = helpers.get_session_laps(session)

            session_weather = helpers.get_weather_data(session)

            session_laps = pd.merge_asof(session_laps, session_weather, on="SessionTime", direction="nearest")

            session_laps_final = helpers.calculate_session_laps_final(session, session_laps)
            
            if session_laps_final.empty:
                print(f"Session laps data is empty for year {year}, track {track}")
                continue
            
            session_results = helpers.add_static_info(session_results)
            # join the session laps result with driver info and final results
            session_laps_final_with_result = pd.merge(session_laps_final, session_results, on='Driver', how='left')

            # session_laps_final_with_result = helpers.add_points(session_laps_final_with_result.copy())

            print(session_laps_final_with_result.head(2))

            final_data = pd.concat([final_data, session_laps_final_with_result], ignore_index=True) if not final_data.empty else session_laps_final_with_result.copy()

            print(" ----- Data loaded ---- ")
            print(f" ----- Year: {year}, Track: {track}, Session: {session.event.Location} loaded ----")
            ff1.Cache.clear_cache()
            print(" ----- Cache cleared ---- ")
            # save data
            final_data = helpers.final_preprocessing(final_data)
            
            # backup export each year and track
            final_data.to_csv(f"final_data_{year}.csv", index=False)

            # Sleep for a while to avoid overloading the server
            time.sleep(5)

    print("\n")
    print(f" ----- Year: {year} data loaded ----")
    print("\n\n\n\n")

