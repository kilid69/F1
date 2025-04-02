import pandas as pd
import numpy as np
import fastf1 as ff1
import time
import helpers
from tqdm import tqdm

# years
years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
# Race sessions
sessions = ['R']

# empty dataframe to store the all seasons and all sessions data
final_data = pd.DataFrame()

for year in tqdm(years):

    for s in sessions:
        # the count of tracks in each year is not over 24
        for track in tqdm(range(1, 25)):
            print(track)
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
            final_data = pd.concat([final_data, session_laps_final], ignore_index=True) if not final_data.empty else session_laps_final.copy()
            
            print(" ----- Data loaded ---- ")
            print(f" ----- Year: {year}, Track: {track}, Session: {session.event.Location} loaded ----")
            ff1.Cache.clear_cache()
            print(" ----- Cache cleared ---- ")
            # Sleep for a while to avoid overloading the server
            time.sleep(5)
            
    # backup export until the full data is ready
    final_data.to_csv(f"final_data_{year}.csv", index=False)

# Save the data to a CSV file
final_data.to_csv("final_data.csv", index=False)