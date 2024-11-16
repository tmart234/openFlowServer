import earthaccess
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
from pathlib import Path
import requests
from dataclasses import dataclass
from smapprocessor import SMAPProcessor
import os

import sqlite3
#from smapprocessor import SMAPProcessor

"""
Soil moisture (SMAP)
Snow (MODIS)
Vegetation (MODIS)
Soil properties (IsricWise)
Terrain (SRTM)
"""

@dataclass
class Station:
    """Single station location data"""
    id: str
    latitude: float
    longitude: float

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def get_usgs_coordinates(site_number: str) -> Optional[Dict]:
    """Get coordinates for a USGS station"""
    base_url = "https://waterdata.usgs.gov/nwis/inventory"
    params = {
        'search_site_no': site_number,
        'search_site_no_match_type': 'exact',
        'group_key': 'NONE',
        'format': 'sitefile_output',
        'sitefile_output_format': 'rdb',
        'column_name': 'site_no,station_nm,dec_lat_va,dec_long_va',  # Fixed: Changed list to comma-separated string
        'list_of_search_criteria': 'search_site_no'
    }
    
    full_url = requests.Request('GET', base_url, params=params).prepare().url
    logger.info(f"Fetching USGS coordinates - URL: {full_url}")
    
    response = requests.get(base_url, params=params)
    lines = response.text.splitlines()
    
    try:
        data_lines = [line for line in lines if not line.startswith('#')]
        if len(data_lines) < 3:
            logger.warning(f"No data found for USGS site {site_number}")
            return None
        
        data = next((line for line in data_lines[2:] if line.split('\t')[0] == site_number), None)
        if data is None:
            logger.warning(f"Site number {site_number} not found in response")
            return None
            
        fields = data.split('\t')
        coordinates = {
            'latitude': float(fields[2]),
            'longitude': float(fields[3])
        }
        logger.info(f"Found USGS coordinates for site {site_number}: lat={coordinates['latitude']}, lon={coordinates['longitude']}")
        return coordinates
        
    except Exception as e:
        logger.error(f"Error getting USGS coordinates for {site_number}: {e}")
        return None
    
def get_dwr_coordinates(abbrev: str) -> Optional[Dict]:
    """Get coordinates for a DWR station"""
    base_url = "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewaterstations"
    params = {
        "format": "json",
        "dateFormat": "dateOnly",
        "fields": "abbrev,longitude,latitude",
        "encoding": "deflate",
        "abbrev": abbrev,
    }
    
    full_url = f"{base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
    logger.info(f"Fetching DWR coordinates - URL: {full_url}")
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Will raise an exception for non-200 status codes
        data = response.json()
        
        if not data.get('ResultList') or len(data['ResultList']) == 0:
            logger.warning(f"No data found for DWR station {abbrev}")
            return None
            
        station = data['ResultList'][0]
        coordinates = {
            'latitude': float(station['latitude']),
            'longitude': float(station['longitude'])
        }
        logger.info(f"Found DWR coordinates for station {abbrev}: lat={coordinates['latitude']}, lon={coordinates['longitude']}")
        return coordinates
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for DWR station {abbrev}: {e}")
        return None
    except (KeyError, ValueError, IndexError) as e:
        logger.error(f"Error parsing response for DWR station {abbrev}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting DWR coordinates for {abbrev}: {e}")
        return None

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

    def __init__(self, db_path: Path):
        self.auth = earthaccess.login(strategy="environment")
        logger.info("Authenticated with NASA Earthdata")
        self.common_start = None
        self.common_end = None
        self.db_path = db_path
        self.setup_database()
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
    
    def setup_database(self):
        """Initialize SQLite database tables for all datasets"""
        logger.info(f"Setting up database at {self.db_path}")
        
        with sqlite3.connect(self.db_path) as conn:
            # Create stations table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stations (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    site_id TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    created_at INTEGER NOT NULL
                )
            ''')
            
            # Create soil moisture features table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS smap_features (
                    timestamp INTEGER,
                    station_id TEXT,
                    soil_moisture REAL,      -- Normalized soil moisture (0-1)
                    quality_score INTEGER,    -- Overall quality (0-100)
                    trend REAL,              -- 3-day trend
                    PRIMARY KEY (timestamp, station_id),
                    FOREIGN KEY (station_id) REFERENCES stations(id)
                )
            ''')
            
            # Create vegetation features table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS vegetation_features (
                    timestamp INTEGER,
                    station_id TEXT,
                    ndvi REAL,             -- Normalized Difference Vegetation Index
                    quality_score INTEGER,  -- Quality indicator
                    PRIMARY KEY (timestamp, station_id),
                    FOREIGN KEY (station_id) REFERENCES stations(id)
                )
            ''')
            
            # Create snow cover features table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS snow_features (
                    timestamp INTEGER,
                    station_id TEXT,
                    snow_cover REAL,        -- Percent snow cover
                    quality_score INTEGER,   -- Quality indicator
                    PRIMARY KEY (timestamp, station_id),
                    FOREIGN KEY (station_id) REFERENCES stations(id)
                )
            ''')
            
            # Create static features table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS static_features (
                    station_id TEXT PRIMARY KEY,
                    elevation REAL,          -- Elevation in meters
                    slope REAL,              -- Terrain slope
                    soil_type TEXT,          -- Soil classification
                    soil_texture TEXT,       -- Soil texture class
                    organic_carbon REAL,     -- Soil organic carbon content
                    clay_content REAL,       -- Clay percentage
                    sand_content REAL,       -- Sand percentage
                    FOREIGN KEY (station_id) REFERENCES stations(id)
                )
            ''')
            
            logger.info("Database tables created successfully")

    def store_stations(self, stations: List[Station]):
        """Store station information in the database"""
        current_time = int(datetime.now().timestamp())
        
        with sqlite3.connect(self.db_path) as conn:
            for station in stations:
                source, site_id = station.id.split(':')
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO stations 
                        (id, source, site_id, latitude, longitude, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        station.id,
                        source,
                        site_id,
                        station.latitude,
                        station.longitude,
                        current_time
                    ))
                    logger.info(f"Stored station {station.id} in database")
                except Exception as e:
                    logger.error(f"Error storing station {station.id}: {e}")

def main():
    # Set up database path
    db_path = Path("data/earth_data.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    analyzer = DatasetAnalyzer(db_path)
    #analyzer.print_coverage_summary()
    common_start, common_end = analyzer.find_common_period()
    
    # Process SMAP data
    processor = SMAPProcessor(analyzer.stations, common_start, common_end)
    SMAPProcessor.readout(db_path)

if __name__ == "__main__":
    main()