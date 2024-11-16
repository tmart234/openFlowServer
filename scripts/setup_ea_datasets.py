import earthaccess
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple, List
from pathlib import Path
import requests
from dataclasses import dataclass
from stations import Station, get_usgs_coordinates, get_dwr_coordinates

from smapprocessor import SMAPProcessor
from init_dbs import setup_database, store_stations

"""
Earthaccess only datasets:
Soil moisture (SMAP)
Snow (MODIS)
Vegetation (MODIS)
Soil properties (IsricWise)
Terrain (SRTM)
"""

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class DatasetAnalyzer:
    SITE_IDS_URL = "https://raw.githubusercontent.com/tmart234/OpenFlow/refs/heads/dev/.github/site_ids.txt"

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
        self.common_start = None
        self.common_end = None
        # Get station pairs from site_ids.txt
        self.stations = self.create_stations()
        logger.debug(f"Created {len(self.stations)} stations")

    def load_site_ids(self) -> List[Tuple[str, str]]:
        """Load site IDs from URL"""
        try:
            response = requests.get(self.SITE_IDS_URL)
            response.raise_for_status()
            site_ids = response.text.strip().split()
            return [tuple(site_id.split(':')) for site_id in site_ids]
        except Exception as e:
            logger.error(f"Error loading site IDs: {e}")
            return []

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
    
    def find_common_period(self) -> Tuple[datetime, datetime]:
        """Find the common time period across all dynamic datasets"""
        latest_start = datetime.min
        earliest_end = datetime.max
        
        for name, params in self.DYNAMIC_DATASETS.items():
            period = self.get_dataset_period(name, params)
            if period:
                start, end = period
                latest_start = max(latest_start, start)
                earliest_end = min(earliest_end, end)
                logger.info(f"{name}: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
        
        self.common_start = latest_start
        self.common_end = earliest_end
        logger.info(f"\nCommon period: {latest_start.strftime('%Y-%m-%d')} to {earliest_end.strftime('%Y-%m-%d')}")
        return latest_start, earliest_end
    
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


    def create_stations(self) -> List[Station]:
        """Create list of stations from site IDs file"""
        site_ids = self.load_site_ids()
        logger.info(f"Loaded {len(site_ids)} site IDs")
        stations = []
        
        # Get coordinates for each station
        for source, site_id in site_ids:
            logger.info(f"Processing station {source}:{site_id}")
            if source.upper() == 'USGS':
                coords = get_usgs_coordinates(site_id)
            elif source.upper() == 'DWR':
                coords = get_dwr_coordinates(site_id)
            else:
                logger.warning(f"Unknown source type: {source}")
                continue
                
            if coords:
                station = Station(
                    id=f"{source}:{site_id}",
                    latitude=coords['latitude'],
                    longitude=coords['longitude']
                )
                stations.append(station)
                logger.info(f"Added station: {station}")
            else:
                logger.warning(f"Could not get coordinates for {source}:{site_id}")
        
        return stations

def main():
    # Set up database path
    db_path = Path("data/earth_data.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    analyzer = DatasetAnalyzer()
    #analyzer.print_coverage_summary()
    common_start, common_end = analyzer.find_common_period()

    setup_database(db_path)
    store_stations(analyzer.stations, db_path)

    # Process SMAP data
    processor = SMAPProcessor(analyzer.stations, common_start, common_end)
    SMAPProcessor.readout(db_path)

if __name__ == "__main__":
    main()