import os
import asyncio
import logging
import sqlite3
from datetime import datetime
from downloadSMAP import main as downloadAndProcessSmap

# Set up logging
LOG_PATH = os.getenv('OPENFLOW_LOG_PATH', '/var/log/openflow_cron.log')
DB_PATH = os.getenv('OPENFLOW_DB_PATH', '/var/lib/openflow/data.db')

# Ensure the directory for the log file exists
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(filename=LOG_PATH, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def find_date_range(results):
    if not results:
        print("No results found to determine date range")
        return None, None

    start_dates = []
    end_dates = []

    for result in results:
        temporal_info = result.time_start, result.time_end
        if all(temporal_info):
            start_dates.append(datetime.strptime(temporal_info[0], "%Y-%m-%dT%H:%M:%S.%fZ"))
            end_dates.append(datetime.strptime(temporal_info[1], "%Y-%m-%dT%H:%M:%S.%fZ"))

    if start_dates and end_dates:
        earliest_date = min(start_dates).strftime("%Y-%m-%d")
        latest_date = max(end_dates).strftime("%Y-%m-%d")
        print(f"Date range: {earliest_date} to {latest_date}")
        return earliest_date, latest_date
    else:
        print("Unable to determine date range from the results")
        return None, None

def store_processed_data(processed_data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS processed_data 
                      (date TEXT, location TEXT, smap_value REAL, vegdri_value REAL)''')
    cursor.executemany('''INSERT INTO processed_data (date, location, smap_value, vegdri_value)
                          VALUES (?, ?, ?, ?)''', processed_data)
    conn.commit()
    conn.close()
    print("Processed data stored in the database")

async def main():
    try:
        downloadAndProcessSmap('2023-01-01', '2023-01-31')
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())