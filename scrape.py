import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from io import StringIO
from data_cleaning import *

# Directory for saving CSV files
raw_save_dir = "./sample_DB/raw_data"
clean_save_dir = "./sample_DB/clean_data"
output_excel_file = "portal_recon.xlsx"

# Ensure the directories exist
os.makedirs(raw_save_dir, exist_ok=True)
os.makedirs(clean_save_dir, exist_ok=True)

# Conference-specific configurations
conferences = [
    # ... Redacted for Privacy Reasons ... 
]

# Column order
column_order = [
    'Player Name', 'Pos', 'School', 'Year', 'Height', 'Weight', 'Notes',
    'Hometown', 'High School', 'AAU Team', 'G', 'GS', 'MP/G', 'PTS/G', 
    'RB/G', 'AST/G', 'STL/G', 'BLK/G', 'TOV/G', 'FG/G', 'FGA/G', 
    'FG%', '3P/G', '3PA/G', '3P%', '2P/G', '2PA/G', '2P%', 
    'eFG%', 'FT/G', 'FTA/G', 'FT%', 'ORB/G', 'DRB/G', 'PF/G'
]

# Process each conference
for conf in conferences:
    success = False
    retries = 3

    for attempt in range(retries):
        response = requests.get(conf["url"])
        response.encoding = 'utf-8'
        
        # Check for successful response
        if response.status_code == 200:
            success = True
            break
        elif response.status_code == 429:
            print(f"{conf['name']}: Rate limit hit. Retrying in {2 ** attempt} seconds...")
            time.sleep(2 ** attempt)
        else:
            print(f"{conf['name']}: Failed to retrieve the webpage. Status code: {response.status_code}")
            break

    if not success:
        print(f"{conf['name']}: Skipping due to repeated errors.")
        continue

    print(f"{conf['name']}: Successful response.")

    # Store HTML content in memory
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find the table
    table = soup.find('table', id='players_per_game')

    if table:
        # Convert HTML Table to a DataFrame
        df = pd.read_html(StringIO(str(table)), header=0)[0]

        # Import given data
        given_df = pd.read_csv(conf["given_file"])

        # Save raw data
        raw_file_path = os.path.join(raw_save_dir, f"raw_{conf['name'].lower().replace(' ', '_')}.csv")
        df.to_csv(raw_file_path, index=False)

        # Clean data
        given_df = given_clean(given_df)
        df = bb_ref_clean(df)
        # Match names correctly
        given_df['Player Name'] = given_df['Player Name'].replace(conf["name_mapping"])

        # Left join the given list of players with basketball reference data
        results = pd.merge(given_df, df, left_on='Player Name', right_on='Player', how='left')
        results = results.drop(columns=['Player'])
        results = results[column_order]

        # Sort by minutes per game (MP/G) in descending order
        if 'MP/G' in results.columns:
            results = results.sort_values(by='MP/G', ascending=False)

        # Save cleaned data
        final_file_path = os.path.join(clean_save_dir, f"{conf['name'].lower().replace(' ', '_')}.csv")
        results.to_csv(final_file_path, index=False)

        print(f"{conf['name']}: Data cleaned and saved.")
    else:
        print(f"{conf['name']}: Table not found.")

    # Delay to avoid hitting rate limits
    time.sleep(2)  # Adjust delay as needed

# combine all of the .csv files into a .xlsx file
combine_csv_to_excel(clean_save_dir, output_excel_file)

