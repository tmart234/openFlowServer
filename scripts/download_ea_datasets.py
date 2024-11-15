import earthaccess
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

"""
Soil moisture (SMAP)
Snow (MODIS)
Vegetation (MODIS)
Soil properties (IsricWise)
Terrain (SRTM)
"""

logging.basicConfig(level=logging.INFO, format='%(message)s')
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
class DatasetAnalyzer:
    DYNAMIC_DATASETS = {
        "soil_moisture": {
            "short_name": "SPL3SMP_E",
            "provider": "NSIDC_ECS",
            "version": "006"
        },
        "snow_cover": {
            "short_name": "MOD10A1",
            "provider": "NSIDC_ECS"
        },
        "vegetation": {
            "short_name": "MOD13Q1",
            "provider": "LPCLOUD"
        }
    }
    
    STATIC_DATASETS = {
        "elevation": {
            "short_name": "SRTMGL1",
            "provider": "LPCLOUD"
        },
        "soil_properties": {
            "short_name": "IsricWiseGrids_546",
            "provider": "ORNL_CLOUD"
        }
    }

    def __init__(self):
        self.auth = earthaccess.login(strategy="environment")
        logger.info("Authenticated with NASA Earthdata")

    def get_dataset_period(self, name: str, params: Dict) -> Optional[Tuple[datetime, datetime]]:
        """Get start and end dates for a dataset using collection metadata"""
        dataset = earthaccess.search_datasets(**params)
        if not dataset:
            logger.warning(f"Dataset not found: {name}")
            return None
            
        collection = dataset[0]
        temporal = collection.get_umm('TemporalExtents')
        
        if not temporal or not temporal[0].get('RangeDateTimes'):
            return None
            
        range_dates = temporal[0]['RangeDateTimes'][0]
        start = datetime.strptime(range_dates['BeginningDateTime'][:10], '%Y-%m-%d')
        
        if range_dates.get('EndingDateTime'):
            end = datetime.strptime(range_dates['EndingDateTime'][:10], '%Y-%m-%d')
        else:
            end = datetime.now()
            
        return start, end

    def print_coverage_summary(self):
        """Print temporal coverage for all dynamic datasets"""
        logger.info("\nDynamic Dataset Coverage Periods:")
        logger.info("-" * 50)
        
        for name, params in self.DYNAMIC_DATASETS.items():
            period = self.get_dataset_period(name, params)
            if period:
                start, end = period
                years = (end - start).days / 365.25
                logger.info(f"{name:15} {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} ({years:.1f} years)")

        logger.info("\nStatic Datasets:")
        logger.info("-" * 50)
        for name in self.STATIC_DATASETS:
            logger.info(f"{name:15} (time-invariant)")

def main():
    analyzer = DatasetAnalyzer()
    analyzer.print_coverage_summary()

if __name__ == "__main__":
    main()