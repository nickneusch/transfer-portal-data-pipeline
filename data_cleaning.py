import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from io import StringIO

def bb_ref_clean(df):

    # Drop unneeded columns
    df = df.drop(columns=['Rk', 'Awards'])
    # Rename columns to fit their 'per game' nature
    df = df.rename(columns={'MP': 'MP/G', 
                            'FG': 'FG/G',
                            'FGA': 'FGA/G',
                            '3P': '3P/G',
                            '3PA': '3PA/G',
                            '2P': '2P/G',
                            '2PA': '2PA/G',
                            'FT': 'FT/G',
                            'FTA': 'FTA/G',
                            'ORB': 'ORB/G',
                            'DRB': 'DRB/G',
                            'TRB': 'RB/G',
                            'AST': 'AST/G',
                            'STL': 'STL/G',
                            'BLK': 'BLK/G',
                            'TOV': 'TOV/G',
                            'PF': 'PF/G',
                            'PTS': 'PTS/G',
                            'Team': 'School'
                            })
    # Change percentage columns from decimals to percentages
    percentage_columns = ['FG%', '3P%', '2P%', 'FT%', 'eFG%']
    df[percentage_columns] = df[percentage_columns].apply(
        lambda col: col.apply(lambda x: f"{x * 100:.2f}" if pd.notna(x) else "")
    )

    return df

def given_clean(df):

    # Remove unneeded columns and rename certain columns
    if 'Notes' not in df.columns:
        df['Notes'] = ''

    # Ensure first column is 'Player Name'
    rename_mapping = {
        'Player Name ': 'Player Name',
        'NAME': 'Player Name',
        'Column1': 'Player Name',
        'Name': 'Player Name'
    }

    # Rename columns only if they exist in the DataFrame
    df = df.rename(columns={col: rename_mapping[col] for col in df.columns if col in rename_mapping})


    # Fix High School
    if 'High School ' in df.columns:
        df = df.rename(columns={'High School ': 'High School'})
    
    # Fix Hometown
    df = df.rename(columns={'Home Town': 'Hometown'})

    # Adjust Column Order
    df = df[['Player Name', 'Year', 'Height', 'Weight', 'Notes', 'Hometown', 'High School', 'AAU Team']]

    # Drop extra rows
    df = df.dropna(subset=['Player Name'])

    return df

# Combine all cleaned .csv files into one .xlsx file
def combine_csv_to_excel(folder_path, output_file):
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        for file in sorted(os.listdir(folder_path)):  # Ensuring consistent order
            if file.endswith(".csv"):
                file_path = os.path.join(folder_path, file)
                df = pd.read_csv(file_path)
                
                # Ensure sorting in case it's needed
                if 'MP/G' in df.columns:
                    df = df.sort_values(by='MP/G', ascending=False)

                sheet_name = os.path.splitext(file)[0][:31]  # Excel sheet names must be <= 31 chars
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Combined Excel file saved as: {output_file}")