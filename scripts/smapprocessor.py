import sqlite3
import time
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple
import h5py
import earthaccess
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class Station:
    """Single station location data"""
    id: str
    latitude: float
    longitude: float

class SMAPProcessor:
    """Processor for SMAP soil moisture data"""
    
    def __init__(self, stations: List[Station], start_date: datetime, end_date: datetime, radius_km: float = 5.0):
        """Initialize with stations and time period"""
        self.stations = stations
        self.start_date = start_date
        self.end_date = end_date
        self.radius_km = radius_km
        self.db_path = Path("data/earth_data.db")
        
        # Initialize auth but don't store it
        try:
            earthaccess.login(strategy="environment")
            logger.info("Authentication successful")
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise
        
        logger.info(f"Initialized SMAP Processor with:")
        logger.info(f"- {len(stations)} stations")
        logger.info(f"- Time period: {start_date.date()} to {end_date.date()}")
        
        # Start processing
        self.process_data()

    def process_data(self):
        """Main processing function with better cleanup"""
        # Create temp directory for downloads
        temp_dir = Path("temp_smap")
        temp_dir.mkdir(exist_ok=True)
        
        try:
            current_date = self.start_date
            while current_date <= self.end_date:
                next_date = current_date + timedelta(days=1)
                logger.info(f"Processing date: {current_date.date()}")
                
                try:
                    # Search for data
                    granules = earthaccess.search_data(
                        short_name="SPL3SMP_E",
                        version="006",
                        provider="NSIDC_ECS",
                        temporal=(current_date, next_date),
                        count=1
                    )
                    
                    if granules:
                        logger.info(f"Found {len(granules)} granules")
                        for granule in granules:
                            try:
                                # Download file
                                downloaded = earthaccess.download(
                                    granule, 
                                    local_path=str(temp_dir)
                                )
                                
                                if downloaded and len(downloaded) > 0:
                                    file_path = downloaded[0]
                                    # Process and immediately delete
                                    self._process_file(file_path)
                                    try:
                                        Path(file_path).unlink()
                                    except Exception as e:
                                        logger.error(f"Error removing file {file_path}: {e}")
                                
                            except Exception as e:
                                logger.error(f"Error processing granule: {e}")
                                continue
                    
                except Exception as e:
                    logger.error(f"Error on {current_date.date()}: {e}")
                
                current_date = next_date
                time.sleep(1)
                
        finally:
            # Clean up temp directory
            try:
                # Force cleanup of any remaining files
                for file in temp_dir.glob('*'):
                    try:
                        file.unlink()
                        logger.debug(f"Cleaned up file: {file}")
                    except Exception as e:
                        logger.error(f"Error removing file {file}: {e}")
                
                # Try to remove directory
                temp_dir.rmdir()
                logger.info("Cleaned up temporary directory")
            except Exception as e:
                logger.error(f"Error cleaning up temp directory: {e}")

    def _process_file(self, file_path: str):
        """Process a single SMAP file with correct filename parsing"""
        logger.info(f"Processing: {file_path}")
        
        try:
            with h5py.File(file_path, 'r', libver='earliest') as f:
                # Define the paths we need (AM data)
                paths = {
                    'soil_moisture': 'Soil_Moisture_Retrieval_Data_AM/soil_moisture',
                    'quality': 'Soil_Moisture_Retrieval_Data_AM/retrieval_qual_flag',
                    'latitude': 'Soil_Moisture_Retrieval_Data_AM/latitude',
                    'longitude': 'Soil_Moisture_Retrieval_Data_AM/longitude'
                }
                
                # Verify all paths exist
                for name, path in paths.items():
                    if path not in f:
                        raise KeyError(f"Required dataset not found: {path}")
                
                # Parse timestamp from filename correctly
                # Example filename: SMAP_L3_SM_P_E_20150331_R19240_001.h5
                filename = Path(file_path).name
                parts = filename.split('_')
                date_str = None
                for part in parts:
                    # Look for 8-digit number that starts with '20'
                    if part.startswith('20') and len(part) == 8 and part.isdigit():
                        date_str = part
                        break
                        
                if not date_str:
                    raise ValueError(f"Could not find date in filename: {filename}")
                    
                logger.debug(f"Found date string: {date_str}")
                timestamp = int(datetime.strptime(date_str, '%Y%m%d').timestamp())
                logger.info(f"Processing data for date: {datetime.fromtimestamp(timestamp).date()}")
                
                # Read required datasets
                sm = f[paths['soil_moisture']][:]
                quality = f[paths['quality']][:]
                lat = f[paths['latitude']][:]
                lon = f[paths['longitude']][:]
                
                logger.info(f"Data shapes - SM: {sm.shape}, Quality: {quality.shape}")
                
                # Process each station
                data_to_insert = []
                for station in self.stations:
                    try:
                        sm_value, quality_score = self._get_station_data(
                            sm, quality, lat, lon,
                            station.latitude, station.longitude
                        )
                        
                        if not np.isnan(sm_value):
                            data_to_insert.append((
                                timestamp,
                                station.id,
                                float(sm_value),
                                int(quality_score * 100)
                            ))
                            logger.info(f"Processed station {station.id}: SM={sm_value:.3f}, Quality={quality_score:.2f}")
                            
                    except Exception as e:
                        logger.error(f"Error processing station {station.id}: {e}")
                        continue
                
                # Save to database
                if data_to_insert:
                    self._save_to_database(data_to_insert)
                    logger.info(f"Saved {len(data_to_insert)} records to database")
                else:
                    logger.warning("No valid data to save")
                    
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _get_station_data(self, sm, quality, lat, lon, target_lat, target_lon) -> Tuple[float, float]:
        """Get soil moisture data for a station location"""
        try:
            # Calculate distances for rough filter
            y_dist = np.abs(lat - target_lat)
            x_dist = np.abs(lon - target_lon)
            
            # Quick filter to reduce computation
            rough_mask = (y_dist <= (self.radius_km / 111.32)) & (x_dist <= (self.radius_km / (111.32 * np.cos(np.deg2rad(target_lat)))))
            
            if not np.any(rough_mask):
                return np.nan, 0.0
            
            # Get masked data
            valid_sm = sm[rough_mask]
            valid_quality = quality[rough_mask]
            
            # Remove fill values and invalid data
            valid_mask = ~np.isnan(valid_sm) & (valid_sm > -999) & (valid_sm < 999)
            valid_sm = valid_sm[valid_mask]
            valid_quality = valid_quality[valid_mask]
            
            if len(valid_sm) == 0:
                return np.nan, 0.0
            
            # Calculate statistics
            mean_sm = np.nanmean(valid_sm)
            quality_pct = np.mean(valid_quality == 0)  # 0 is good quality in SMAP data
            
            return mean_sm, quality_pct
            
        except Exception as e:
            logger.error(f"Error in _get_station_data: {e}")
            return np.nan, 0.0
    
    @classmethod
    def readout(cls, db_path: Path = Path("data/earth_data.db")):
        """Read and display SMAP data from database"""
        logger.info("\nSMAP Data Readout")
        logger.info("-" * 50)
        
        try:
            with sqlite3.connect(db_path) as conn:
                # Get overall statistics
                stats = conn.execute('''
                    SELECT 
                        COUNT(DISTINCT station_id) as station_count,
                        COUNT(*) as total_records,
                        MIN(datetime(timestamp, 'unixepoch')) as earliest_date,
                        MAX(datetime(timestamp, 'unixepoch')) as latest_date,
                        AVG(soil_moisture) as avg_moisture,
                        AVG(quality_score) as avg_quality
                    FROM smap_features
                ''').fetchone()
                
                if stats[0] == 0:
                    logger.info("No SMAP data found in database")
                    return
                    
                logger.info(f"Overall Statistics:")
                logger.info(f"- Stations with data: {stats[0]}")
                logger.info(f"- Total measurements: {stats[1]}")
                logger.info(f"- Date range: {stats[2]} to {stats[3]}")
                logger.info(f"- Average soil moisture: {stats[4]:.3f}")
                logger.info(f"- Average quality score: {stats[5]:.1f}")
                
                # Get per-station statistics
                logger.info("\nPer-Station Statistics:")
                logger.info("-" * 50)
                
                station_stats = conn.execute('''
                    SELECT 
                        s.id,
                        s.source,
                        s.site_id,
                        COUNT(f.timestamp) as measurements,
                        AVG(f.soil_moisture) as avg_moisture,
                        AVG(f.quality_score) as avg_quality,
                        MIN(datetime(f.timestamp, 'unixepoch')) as first_date,
                        MAX(datetime(f.timestamp, 'unixepoch')) as last_date
                    FROM stations s
                    LEFT JOIN smap_features f ON s.id = f.station_id
                    GROUP BY s.id
                    ORDER BY s.source, s.site_id
                ''').fetchall()
                
                current_source = None
                for stat in station_stats:
                    # Print source header if changed
                    if stat[1] != current_source:
                        current_source = stat[1]
                        logger.info(f"\n{current_source} Stations:")
                    
                    # Format station information
                    if stat[3] > 0:  # If station has data
                        logger.info(
                            f"Station {stat[2]}: "
                            f"{stat[3]} measurements, "
                            f"moisture={stat[4]:.3f}, "
                            f"quality={stat[5]:.1f}, "
                            f"period={stat[6]} to {stat[7]}"
                        )
                    else:
                        logger.info(f"Station {stat[2]}: No data")
                
                # Get recent readings
                logger.info("\nMost Recent Readings:")
                logger.info("-" * 50)
                
                recent = conn.execute('''
                    WITH RankedReadings AS (
                        SELECT 
                            station_id,
                            datetime(timestamp, 'unixepoch') as reading_time,
                            soil_moisture,
                            quality_score,
                            ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY timestamp DESC) as rn
                        FROM smap_features
                    )
                    SELECT *
                    FROM RankedReadings
                    WHERE rn = 1
                    ORDER BY reading_time DESC
                    LIMIT 5
                ''').fetchall()
                
                for reading in recent:
                    logger.info(
                        f"{reading[0]}: "
                        f"moisture={reading[2]:.3f}, "
                        f"quality={reading[3]}, "
                        f"time={reading[1]}"
                    )
                
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
        except Exception as e:
            logger.error(f"Error during readout: {e}")

    def _save_to_database(self, data: List[Tuple]):
        """Save processed data to database"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.executemany('''
                    INSERT OR REPLACE INTO smap_features 
                    (timestamp, station_id, soil_moisture, quality_score)
                    VALUES (?, ?, ?, ?)
                ''', data)
                logger.debug(f"Saved {len(data)} records to database")
            except Exception as e:
                logger.error(f"Database error: {e}")