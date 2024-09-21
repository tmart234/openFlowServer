import os
import earthaccess
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(filename='vegdri_search.log', level=logging.INFO,
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
        results = earthaccess.search_data(
            short_name="VegDRI",
            cloud_hosted=True
        )
        logging.info(f"Found {len(results)} VegDRI dataset results")
        return results
    except Exception as e:
        logging.error(f"Error searching for VegDRI dataset: {str(e)}")
        raise

def find_date_range(results):
    if not results:
        logging.warning("No results found to determine date range")
        return None, None

    start_dates = []
    end_dates = []

    for result in results:
        temporal_info = result.get('time_start'), result.get('time_end')
        if all(temporal_info):
            start_dates.append(datetime.strptime(temporal_info[0], "%Y-%m-%dT%H:%M:%S.%fZ"))
            end_dates.append(datetime.strptime(temporal_info[1], "%Y-%m-%dT%H:%M:%S.%fZ"))

    if start_dates and end_dates:
        earliest_date = min(start_dates).strftime("%Y-%m-%d")
        latest_date = max(end_dates).strftime("%Y-%m-%d")
        logging.info(f"Date range: {earliest_date} to {latest_date}")
        return earliest_date, latest_date
    else:
        logging.warning("Unable to determine date range from the results")
        return None, None

def main():
    try:
        auth = authenticate()
        results = search_vegdri_dataset()
        earliest_date, latest_date = find_date_range(results)

        if earliest_date and latest_date:
            print(f"VegDRI dataset available date range: {earliest_date} to {latest_date}")
        else:
            print("Unable to determine VegDRI dataset date range")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print(f"An error occurred. Check the log file for details.")

if __name__ == "__main__":
    main()