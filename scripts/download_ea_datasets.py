import earthaccess
import logging
from datetime import datetime, timedelta

"""
Soil moisture (SMAP)
Snow (MODIS)
Vegetation (MODIS)
Soil properties (IsricWise)
Terrain (SRTM)
"""

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Authentication
try:
    auth = earthaccess.login(strategy="environment")
    logger.info("Successfully authenticated with NASA Earthdata")
except Exception as e:
    logger.error(f"Authentication failed: {str(e)}")
    raise
class DatasetConfig:
    DYNAMIC = {
        "soil_moisture_l3": {
            "short_name": "SPL3SMP_E",
            "provider": "NSIDC_ECS",
            "version": "006"
        },
        "snow_cover": {
            "short_name": "MOD10A1",
            "provider": "NSIDC_ECS"
        },
        "vegetation_indices": {
            "short_name": "MOD13Q1",
            "provider": "LPCLOUD"
        }
    }
    
    STATIC = {
        "elevation": {
            "short_name": "SRTMGL1",
            "provider": "LPCLOUD"
        },
        "soil_properties": {
            "short_name": "IsricWiseGrids_546",
            "provider": "ORNL_CLOUD"
        }
    }

def search_dynamic_datasets():
    """Search for dynamic (frequently updated) datasets"""
    logger.info("\nSearching DYNAMIC Datasets:")
    found_datasets = {}
    
    for data_type, params in DatasetConfig.DYNAMIC.items():
        try:
            logger.info(f"\nSearching for {data_type}...")
            datasets = earthaccess.search_datasets(**params)
            
            if datasets:
                found_datasets[data_type] = datasets
                logger.info(f"Found {len(datasets)} datasets")
                for dataset in datasets:
                    logger.info(f"Dataset: {dataset.get_umm('ShortName')}")
                    logger.info(f"Concept ID: {dataset.concept_id()}")
                    logger.info(f"Version: {dataset.version()}")
                    logger.info(f"Cloud hosted: {'Yes' if dataset.s3_bucket() else 'No'}")
            else:
                logger.info(f"No datasets found for {data_type}")
                
        except Exception as e:
            logger.error(f"Error searching for {data_type}: {e}")
            
    return found_datasets

def search_static_datasets():
    """Search for static (unchanging) datasets"""
    logger.info("\nSearching STATIC Datasets:")
    found_datasets = {}
    
    for data_type, params in DatasetConfig.STATIC.items():
        try:
            logger.info(f"\nSearching for {data_type}...")
            datasets = earthaccess.search_datasets(**params)
            
            if datasets:
                found_datasets[data_type] = datasets
                logger.info(f"Found {len(datasets)} datasets")
                for dataset in datasets:
                    logger.info(f"Dataset: {dataset.get_umm('ShortName')}")
                    logger.info(f"Concept ID: {dataset.concept_id()}")
                    logger.info(f"Version: {dataset.version()}")
            else:
                logger.info(f"No datasets found for {data_type}")
                
        except Exception as e:
            logger.error(f"Error searching for {data_type}: {e}")
            
    return found_datasets

def get_recent_data(dynamic_datasets, days_back=30, bounding_box=None):
    """Get recent data for dynamic datasets"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    logger.info(f"\nSearching for data between {start_date.date()} and {end_date.date()}")
    recent_data = {}
    
    for data_type, datasets in dynamic_datasets.items():
        if datasets:
            dataset = datasets[0]
            try:
                search_params = {
                    "concept_id": dataset.concept_id(),
                    "temporal": (start_date.strftime("%Y-%m-%d"), 
                               end_date.strftime("%Y-%m-%d"))
                }
                
                if bounding_box:
                    search_params["bounding_box"] = bounding_box
                
                granules = earthaccess.search_data(**search_params)
                
                if granules:  # Check if granules exist before accessing
                    recent_data[data_type] = granules
                    logger.info(f"\nFound {len(granules)} granules for {data_type}")
                    logger.info(f"Sample granule size: {granules[0].size():.2f} MB")
                else:
                    logger.warning(f"\nNo granules found for {data_type} between {start_date.date()} and {end_date.date()}")
                    if bounding_box:
                        logger.warning(f"Check if {data_type} has coverage in specified bounding box")
                    
            except Exception as e:
                logger.error(f"Error getting granules for {data_type}: {e}")
                logger.debug(f"Full error: ", exc_info=True)  # Add detailed error info
                
    return recent_data

def main():
    # Example usage with bounding box for Colorado
    colorado_bbox = (-109.0, 37.0, -102.0, 41.0)  # (min_lon, min_lat, max_lon, max_lat)

    # Search for all datasets
    dynamic_datasets = search_dynamic_datasets()
    static_datasets = search_static_datasets()

    # Get recent data for dynamic datasets
    recent_data = get_recent_data(dynamic_datasets, days_back=30, bounding_box=colorado_bbox)

    # Summarize data availability
    logger.info("\nData Availability Summary:")
    for data_type, granules in recent_data.items():
        if granules:
            logger.info(f"\n{data_type}:")
            logger.info(f"Number of granules: {len(granules)}")
            logger.info(f"Total size: {sum(g.size() for g in granules):.2f} MB")

if __name__ == "__main__":
    main()