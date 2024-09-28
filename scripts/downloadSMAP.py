import datetime
import logging
from earthaccess import *
import os
import tempfile
import h5py
import numpy as np
import gc
from memory_profiler import profile

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_smap_file(granule, temp_dir):
    """Download a single SMAP file to the specified temporary directory"""
    try:
        auth = login(strategy="environment")
        logger.info("Successfully authenticated with NASA Earthdata")
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise

    try:
        downloaded_files = download(granule, local_path=temp_dir)
        if downloaded_files:
            downloaded_file = downloaded_files[0]
            logger.info(f"Downloaded: {os.path.basename(downloaded_file)} to {temp_dir}")
            return downloaded_file
        else:
            logger.error(f"Failed to download granule")
            return None
    except Exception as e:
        logger.error(f"Error downloading granule: {str(e)}")
        return None

def process_soil_moisture_dataset(dataset, group_name):
    """Process a soil moisture dataset"""
    logger.info(f"Processing {group_name} - Shape: {dataset.shape}, Dtype: {dataset.dtype}")
    
    # Here you would add your processing logic
    # For example, you might want to calculate statistics or apply some transformations
    mean_soil_moisture = np.mean(dataset[:])
    std_soil_moisture = np.std(dataset[:])
    
    logger.info(f"{group_name} - Mean soil moisture: {mean_soil_moisture:.4f}, Std: {std_soil_moisture:.4f}")

def process_smap(file_path):
    """Process the SMAP file"""
    try:
        with h5py.File(file_path, 'r') as h5_file:
            # Process AM data
            if 'Soil_Moisture_Retrieval_Data_AM' in h5_file:
                am_group = h5_file['Soil_Moisture_Retrieval_Data_AM']
                if 'soil_moisture' in am_group:
                    process_soil_moisture_dataset(am_group['soil_moisture'], 'AM')
            
            # Process PM data
            if 'Soil_Moisture_Retrieval_Data_PM' in h5_file:
                pm_group = h5_file['Soil_Moisture_Retrieval_Data_PM']
                if 'soil_moisture_dca_pm' in pm_group:
                    process_soil_moisture_dataset(pm_group['soil_moisture_dca_pm'], 'PM')
            
            # Process Polar AM data
            if 'Soil_Moisture_Retrieval_Data_Polar_AM' in h5_file:
                polar_am_group = h5_file['Soil_Moisture_Retrieval_Data_Polar_AM']
                if 'soil_moisture' in polar_am_group:
                    process_soil_moisture_dataset(polar_am_group['soil_moisture'], 'Polar AM')
            
            # Process Polar PM data
            if 'Soil_Moisture_Retrieval_Data_Polar_PM' in h5_file:
                polar_pm_group = h5_file['Soil_Moisture_Retrieval_Data_Polar_PM']
                if 'soil_moisture_dca_pm' in polar_pm_group:
                    process_soil_moisture_dataset(polar_pm_group['soil_moisture_dca_pm'], 'Polar PM')
            
            logger.info(f"Successfully processed file: {file_path}")
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")

@profile
def main(start_date, end_date):
    # Convert string dates to datetime.date objects
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    # Search for SPL3SMP_E collection
    collection_query = DataCollections().short_name("SPL3SMP_E").version("006")
    collections = collection_query.get()

    if not collections:
        logger.error("SMAP L3 SM_P_E collection not found")
        return

    collection = collections[0]
    concept_id = collection.concept_id()

    # Search for granules
    granule_query = (DataGranules()
                     .concept_id(concept_id) 
                     .temporal(start_date, end_date))

    granules = granule_query.get()

    if not granules:
        logger.warning(f"No granules found for the specified date range")
        return

    logger.info(f"Found {len(granules)} granules")

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Process granules one at a time
            for granule in granules:
                downloaded_file = download_smap_file(granule, temp_dir)
                if downloaded_file:
                    process_smap(downloaded_file)
                    os.remove(downloaded_file)
                    logger.info(f"Processed and removed file: {downloaded_file}")
                    gc.collect()  # Force garbage collection
        except Exception as e:
            logger.error(f"An error occurred during processing: {str(e)}")

    logger.info("All files have been processed.")

if __name__ == "__main__":
    # Example usage
    main('2023-01-01', '2023-01-31')