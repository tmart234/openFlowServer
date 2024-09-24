import os
import asyncio
import aiohttp
import earthaccess
import logging
import sqlite3
from datetime import datetime, timedelta
from tqdm import tqdm
import tempfile

# Set up logging
LOG_PATH = os.getenv('OPENFLOW_LOG_PATH', '/var/log/openflow_cron.log')
DB_PATH = os.getenv('OPENFLOW_DB_PATH', '/var/lib/openflow/data.db')

# Ensure the directory for the log file exists
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(filename=LOG_PATH, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def authenticate():
    try:
        auth = earthaccess.login(strategy="environment")
        logging.info("Successfully authenticated with NASA Earthdata")
        return auth
    except Exception as e:
        logging.error(f"Authentication failed: {str(e)}")
        raise

def search_vegdri_dataset():
    try:
        results = earthaccess.search_datasets(
            short_name="VegDRI",
            cloud_hosted=True
        )
        logging.info(f"Found {len(results)} VegDRI dataset results")
        if len(results) == 0:
            logging.warning("No VegDRI datasets found. This may indicate an issue with search parameters or data availability.")
        return results
    except Exception as e:
        logging.error(f"Error searching for VegDRI dataset: {str(e)}")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error(f"Error args: {e.args}")
        raise

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

def update_vegdri_metadata(start_date, end_date):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS vegdri_metadata (start_date TEXT, end_date TEXT)")
        cursor.execute("DELETE FROM vegdri_metadata")
        cursor.execute("INSERT INTO vegdri_metadata (start_date, end_date) VALUES (?, ?)", (start_date, end_date))
        conn.commit()
        print("Successfully updated VegDRI metadata in the database")
    except Exception as e:
        print(f"Error updating database: {str(e)}")
        raise
    finally:
        conn.close()

async def download_file(session, url, temp_file):
    async with session.get(url) as response:
        content = await response.read()
        temp_file.write(content)
        temp_file.flush()
        return temp_file.name

def process_file(file_path, file_date):
    # Process the file
    # This is a placeholder for the actual processing logic
    print(f"Processing file for date: {file_date}")
    # Return dummy processed data
    return [(file_date, "Location1", 0.5, 0.7), (file_date, "Location2", 0.6, 0.8)]

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

async def process_date(session, date, pbar):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        try:
            # Search for data granules for the specific date
            granules = earthaccess.search_data(
                short_name="VegDRI",
                temporal=(date, date),
                cloud_hosted=True
            )

            if not granules:
                print(f"No granules found for date: {date}")
                return

            # Download file
            # Note: This assumes the first granule has the data we need. You might need to adjust this logic.
            url = granules[0].data_links()[0]
            await download_file(session, url, temp_file)
            
            # Process file
            processed_data = process_file(temp_file.name, date)
            
            # Store processed data
            store_processed_data(processed_data)
            
            pbar.update(1)
        finally:
            # Delete temp file
            os.unlink(temp_file.name)

async def process_queue(queue, total_tasks):
    async with aiohttp.ClientSession() as session:
        with tqdm(total=total_tasks, desc="Processing dates", unit="date") as pbar:
            while not queue.empty():
                date = await queue.get()
                await process_date(session, date, pbar)
                queue.task_done()

async def main():
    try:
        auth = authenticate()
        results = search_vegdri_dataset()
        start_date, end_date = find_date_range(results)

        if start_date and end_date:
            update_vegdri_metadata(start_date, end_date)
            print(f"VegDRI dataset available date range: {start_date} to {end_date}")
            
            # Create a queue of dates to process
            queue = asyncio.Queue()
            current_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            total_days = (end_date - current_date).days + 1
            
            while current_date <= end_date:
                queue.put_nowait(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)
            
            # Process the queue
            await process_queue(queue, total_days)
        else:
            print("Unable to determine VegDRI dataset date range")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())