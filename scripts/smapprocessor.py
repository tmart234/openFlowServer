import sqlite3
import time
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict
import h5py
import earthaccess
from dataclasses import dataclass

from stations import Station

logger = logging.getLogger(__name__)

class SMAPProcessor:
    """Processor for SMAP soil moisture data"""
    
    def __init__(self, stations: List[Station], start_date: datetime, end_date: datetime, 
                radius_km: float = 5.0, 
                frozen_soil_threshold: float = 273.15,
                chunk_size: int = 50, # Number of pixels to process at once
                vegetation_threshold: float = 5.0,
                watershed_file: Optional[Path] = None,
                dem_file: Optional[Path] = None):
        """
        Initialize SMAP processor with enhanced hydrological parameters
        
        Args:
            stations: List of measurement stations
            start_date: Processing start date
            end_date: Processing end date
            radius_km: Search radius for pixels
            frozen_soil_threshold: Temperature threshold for frozen soil (Kelvin)
            vegetation_threshold: Maximum vegetation water content (kg/m^2)
            watershed_file: Optional GIS file with watershed boundaries
            dem_file: Optional Digital Elevation Model file
            chunk_size: Number of pixels to process at once

        """
        self.stations = stations
        self.start_date = start_date
        self.end_date = end_date
        self.radius_km = radius_km
        self.chunk_size = chunk_size
        self.frozen_soil_threshold = frozen_soil_threshold
        self.vegetation_threshold = vegetation_threshold
        
        # Load watershed boundaries if provided
        self.watersheds = None
        if watershed_file and watershed_file.exists():
            try:
                import geopandas as gpd
                self.watersheds = gpd.read_file(watershed_file)
                logger.info(f"Loaded watershed boundaries for {len(self.watersheds)} catchments")
            except Exception as e:
                logger.error(f"Failed to load watershed file: {e}")
        
        # Load DEM if provided for topographic corrections
        self.dem = None
        if dem_file and dem_file.exists():
            try:
                import rasterio
                with rasterio.open(dem_file) as src:
                    self.dem = src.read(1)
                    self.dem_transform = src.transform
                logger.info("Loaded DEM for topographic corrections")
            except Exception as e:
                logger.error(f"Failed to load DEM: {e}")
        
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
        logger.info(f"- Using watershed boundaries: {watershed_file is not None}")
        logger.info(f"- Using DEM: {dem_file is not None}")
        
        # Start processing
        self.process_data()


    def process_data(self):
        """Process SMAP data day by day, combining AM and PM granules"""
        temp_dir = Path("temp_smap")
        temp_dir.mkdir(exist_ok=True)
        
        try:
            current_date = self.start_date
            while current_date <= self.end_date:
                next_date = current_date + timedelta(days=1)
                logger.info(f"Processing date: {current_date.date()}")
                
                try:
                    # Get both AM and PM granules for the day
                    granules = earthaccess.search_data(
                        short_name="SPL3SMP_E",
                        version="006",
                        provider="NSIDC_ECS",
                        temporal=(current_date, next_date),
                        count=2
                    )
                    
                    if not granules:
                        logger.warning(f"No granules found for {current_date.date()}")
                        current_date = next_date
                        continue
                    
                    # Process and combine AM/PM data
                    daily_data = self._process_daily_granules(granules, temp_dir, current_date)
                    
                    if daily_data:
                        self._save_daily_data(daily_data)
                        logger.info(f"Saved daily data for {current_date.date()}")
                    
                except Exception as e:
                    logger.error(f"Error processing {current_date.date()}: {e}")
                
                current_date = next_date
                time.sleep(1)  # Rate limiting
                
        finally:
            # Cleanup
            for file in temp_dir.glob('*'):
                try:
                    file.unlink()
                except Exception as e:
                    logger.error(f"Error removing file {file}: {e}")
            try:
                temp_dir.rmdir()
            except Exception as e:
                logger.error(f"Error cleaning temp directory: {e}")

    def _save_daily_data(self, daily_data: Dict[str, Dict]):
        """Save daily data to database"""
        try:
            with sqlite3.connect("data/earth_data.db") as conn:
                for data in daily_data.values():
                    conn.execute('''
                        INSERT OR REPLACE INTO smap_features 
                        (timestamp, station_id, soil_moisture, quality_flag)
                        VALUES (:timestamp, :station_id, :soil_moisture, :quality_flag)
                    ''', data)
                
                logger.info(f"Saved {len(daily_data)} records to database")
                
        except Exception as e:
            logger.error(f"Error saving daily data: {e}")

    def _process_daily_granules(self, granules: List, temp_dir: Path, 
                            date: datetime) -> Dict[str, Dict]:
        """Process AM and PM granules for a single day and combine the data"""
        daily_data = {}
        am_data = None
        pm_data = None
        
        logger.info(f"Processing {len(granules)} granules for {date.date()}")
        
        for granule in granules:
            try:
                # Examine granule filename from downloaded file
                downloaded = earthaccess.download(granule, local_path=str(temp_dir))
                if not downloaded:
                    logger.warning("Failed to download granule")
                    continue
                    
                file_path = downloaded[0]
                file_name = Path(file_path).name
                is_am = '_AM_' in file_name or '_A_' in file_name
                is_pm = '_PM_' in file_name or '_P_' in file_name
                
                if not (is_am or is_pm):
                    logger.warning(f"Could not determine AM/PM for file: {file_name}")
                    continue

                try:
                    # Process single granule
                    if is_am:
                        am_data = self._process_granule(file_path, True)
                        if am_data:
                            logger.info(f"Successfully processed AM data with {len(am_data)} stations")
                    else:
                        pm_data = self._process_granule(file_path, False)
                        if pm_data:
                            logger.info(f"Successfully processed PM data with {len(pm_data)} stations")
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_name}: {e}")
                finally:
                    # Clean up file immediately after processing
                    try:
                        Path(file_path).unlink()
                    except Exception as e:
                        logger.warning(f"Could not delete temporary file {file_path}: {e}")
                
            except Exception as e:
                logger.error(f"Granule processing error: {str(e)}")
                continue
        
        # Combine AM and PM data for each station
        combined_stations = set()
        timestamp = int(date.timestamp())
        
        for station in self.stations:
            station_am = am_data.get(station.id) if am_data else None
            station_pm = pm_data.get(station.id) if pm_data else None
            
            if station_am or station_pm:
                combined_data = self._combine_am_pm_data(timestamp, station.id, station_am, station_pm)
                if combined_data:
                    daily_data[station.id] = combined_data
                    combined_stations.add(station.id)
        
        if combined_stations:
            logger.info(f"Successfully processed {len(combined_stations)} stations for {date.date()} (AM/PM combined)")
        else:
            logger.warning(f"No data processed for any stations on {date.date()}")
            
        return daily_data

    def _process_granule(self, file_path: str, is_am: bool) -> Dict[str, Dict]:
        """Process a single SMAP granule file"""
        data = {}
        try:
            with h5py.File(file_path, 'r') as f:
                # Select correct paths based on AM/PM
                if is_am:
                    base_path = 'Soil_Moisture_Retrieval_Data_AM'
                    # Try both versions of AM paths that might exist
                    possible_sm_paths = ['soil_moisture', 'soil_moisture_am']
                    possible_qual_paths = ['retrieval_qual_flag', 'retrieval_qual_flag_am']
                    lat_path = 'latitude'
                    lon_path = 'longitude'
                else:
                    base_path = 'Soil_Moisture_Retrieval_Data_PM'
                    possible_sm_paths = ['soil_moisture_dca_pm', 'soil_moisture_pm']
                    possible_qual_paths = ['retrieval_qual_flag_dca_pm', 'retrieval_qual_flag_pm']
                    lat_path = 'latitude_pm'
                    lon_path = 'longitude_pm'
                
                # Find correct soil moisture path
                sm_path = None
                for path in possible_sm_paths:
                    full_path = f'{base_path}/{path}'
                    if full_path in f:
                        sm_path = path
                        break
                
                # Find correct quality flag path
                qual_path = None
                for path in possible_qual_paths:
                    full_path = f'{base_path}/{path}'
                    if full_path in f:
                        qual_path = path
                        break
                
                if not sm_path or not qual_path:
                    logger.error(f"Could not find valid paths for {'AM' if is_am else 'PM'} data")
                    # Print available paths for debugging
                    logger.info("Available paths:")
                    f.visit(lambda x: logger.info(x))
                    return data
                
                # Construct full paths
                paths = {
                    'soil_moisture': f'{base_path}/{sm_path}',
                    'retrieval_qual_flag': f'{base_path}/{qual_path}',
                    'latitude': f'{base_path}/{lat_path}',
                    'longitude': f'{base_path}/{lon_path}'
                }
                
                # Load datasets with error checking
                try:
                    datasets = {}
                    for key, path in paths.items():
                        if path not in f:
                            logger.error(f"Path not found: {path}")
                            raise KeyError(f"Missing required dataset: {path}")
                        datasets[key] = f[path][:]
                    
                    # Log the shape and range of data
                    logger.info(f"Data ranges for {'AM' if is_am else 'PM'} granule:")
                    logger.info(f"Soil moisture shape: {datasets['soil_moisture'].shape}")
                    logger.info(f"Soil moisture range: {np.nanmin(datasets['soil_moisture']):.3f} to {np.nanmax(datasets['soil_moisture']):.3f}")
                    logger.info(f"Quality flags range: {np.nanmin(datasets['retrieval_qual_flag'])} to {np.nanmax(datasets['retrieval_qual_flag'])}")
                    
                    # Process each station
                    for station in self.stations:
                        try:
                            logger.info(f"Processing station {station.id} at ({station.latitude}, {station.longitude})")
                            result = self._get_station_data(
                                datasets['soil_moisture'],
                                datasets['retrieval_qual_flag'],
                                datasets['latitude'],
                                datasets['longitude'],
                                station.latitude,
                                station.longitude
                            )
                            
                            if result is not None:
                                sm_value, quality_flag = result
                                if not np.isnan(sm_value):
                                    data[station.id] = {
                                        'soil_moisture': float(sm_value),
                                        'quality_flag': int(quality_flag)
                                    }
                                    logger.info(f"Successfully processed {station.id}: moisture={sm_value:.3f}, quality={quality_flag}")
                                else:
                                    logger.warning(f"No valid soil moisture value for station {station.id}")
                            else:
                                logger.warning(f"No data found within radius for station {station.id}")
                                
                        except Exception as e:
                            logger.error(f"Error processing station {station.id}: {e}")
                            continue
                    
                except Exception as e:
                    logger.error(f"Error loading datasets: {e}")
                    raise
                    
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
        
        return data

    def _combine_am_pm_data(self, timestamp: int, station_id: str,
                        am_data: Optional[Dict], pm_data: Optional[Dict]) -> Optional[Dict]:
        """Combine AM and PM data for a station, preferring higher quality data"""
        try:
            if am_data and pm_data:
                # If both available, use the one with better quality
                # If same quality, use their average
                if am_data['quality_flag'] < pm_data['quality_flag']:
                    soil_moisture = am_data['soil_moisture']
                    quality_flag = am_data['quality_flag']
                elif pm_data['quality_flag'] < am_data['quality_flag']:
                    soil_moisture = pm_data['soil_moisture']
                    quality_flag = pm_data['quality_flag']
                else:
                    soil_moisture = (am_data['soil_moisture'] + pm_data['soil_moisture']) / 2
                    quality_flag = am_data['quality_flag']  # Same as PM flag
            else:
                # Use whichever is available
                data = am_data if am_data else pm_data
                if not data:
                    return None
                soil_moisture = data['soil_moisture']
                quality_flag = data['quality_flag']
            
            return {
                'timestamp': timestamp,
                'station_id': station_id,
                'soil_moisture': float(soil_moisture),
                'quality_flag': int(quality_flag)
            }
            
        except Exception as e:
            logger.error(f"Error combining AM/PM data for station {station_id}: {e}")
            return None
    
    def _normalize_soil_moisture(self, value: float) -> float:
        """Normalize soil moisture to 0-1 range"""
        if np.isnan(value):
            return 0.0
        # SMAP values typically range from 0 to 0.5 m³/m³
        return min(1.0, max(0.0, value / 0.5))
    
    @classmethod
    def readout(cls, db_path: Path = Path("data/earth_data.db")):
        """Display SMAP data summary from database"""
        logger.info("\nSMAP Data Summary")
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
                        SUM(CASE WHEN quality_flag = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as good_quality_pct
                    FROM smap_features
                ''').fetchone()
                
                if stats[0] == 0:
                    logger.info("No SMAP data found in database")
                    return
                
                logger.info(f"Overall Statistics:")
                logger.info(f"- Stations with data: {stats[0]}")
                logger.info(f"- Total daily measurements: {stats[1]}")
                logger.info(f"- Date range: {stats[2]} to {stats[3]}")
                logger.info(f"- Average soil moisture: {stats[4]:.3f}")
                logger.info(f"- Good quality data: {stats[5]:.1f}%")
                
                # Per-station statistics
                logger.info("\nPer-Station Statistics:")
                for stat in conn.execute('''
                    SELECT 
                        s.id,
                        COUNT(f.timestamp) as measurements,
                        AVG(f.soil_moisture) as avg_moisture,
                        SUM(CASE WHEN f.quality_flag = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as good_quality_pct
                    FROM stations s
                    LEFT JOIN smap_features f ON s.id = f.station_id
                    GROUP BY s.id
                    ORDER BY s.id
                '''):
                    if stat[1] > 0:
                        logger.info(
                            f"{stat[0]}: {stat[1]} days, "
                            f"avg moisture={stat[2]:.3f}, "
                            f"good quality={stat[3]:.1f}%"
                        )
                    else:
                        logger.info(f"{stat[0]}: No data")
                
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")

    def _calculate_trend(self, station_id: str, timestamp: int, current_value: float) -> float:
        """Calculate 3-day trend more efficiently"""
        with sqlite3.connect(self.db_path) as conn:
            # Get one aggregate query instead of fetching all records
            result = conn.execute('''
                SELECT AVG(soil_moisture)
                FROM (
                    SELECT soil_moisture
                    FROM smap_features
                    WHERE station_id = ? AND timestamp < ?
                    ORDER BY timestamp DESC
                    LIMIT 3
                )
            ''', (station_id, timestamp)).fetchone()
            
            if result[0] is not None:
                return current_value - result[0]
                
            return 0.0


    def _get_station_data(self, sm, quality, lat, lon, target_lat, target_lon):
        """Calculate soil moisture for station location using proper data masking"""
        if sm.size > self.chunk_size:
            return self._get_station_data_chunked(sm, quality, lat, lon, target_lat, target_lon)
            
        # Mask fill values
        valid_data = np.logical_and(
            sm != -9999.0,
            np.logical_and(
                sm >= 0.0,
                sm <= 1.0  # SMAP valid range
            )
        )
        
        # Convert to radians
        lat1 = np.deg2rad(target_lat)
        lon1 = np.deg2rad(target_lon)
        lat2 = np.deg2rad(lat)
        lon2 = np.deg2rad(lon)
        
        # Haversine formula components
        dlat = np.abs(lat2 - lat1)
        dlon = np.abs(lon2 - lon1)
        
        # Stable haversine formula implementation
        a = np.sin(dlat/2)**2
        b = np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = np.clip(a + b, 0, 1)
        
        distances = 2 * 6371 * np.arcsin(np.sqrt(c))
        
        # Combine distance and data quality masks
        weight_mask = np.logical_and(
            distances <= self.radius_km,
            valid_data
        )
        
        if not np.any(weight_mask):
            logger.debug(f"No valid pixels within {self.radius_km}km radius")
            return np.nan, 0.0
        
        # Count valid pixels
        n_valid = np.sum(weight_mask)
        if n_valid < 3:  # Require at least 3 valid pixels
            logger.debug(f"Only {n_valid} valid pixels found, need at least 3")
            return np.nan, 0.0
            
        # Calculate weights based on distance and quality
        distances_valid = distances[weight_mask]
        weights = 1 / (distances_valid + 0.1)
        quality_weights = np.where(quality[weight_mask] == 0, 1.0, 0.5)
        final_weights = weights * quality_weights
        
        # Calculate weighted average
        sm_value = np.average(sm[weight_mask], weights=final_weights)
        quality_score = np.average(quality[weight_mask] == 0, weights=weights) * 100
        
        # Add debug info
        logger.debug(
            f"Found {n_valid} valid pixels within {self.radius_km}km. "
            f"Average distance: {np.mean(distances_valid):.1f}km"
        )
        
        return sm_value, int(quality_score)

    def _process_granule(self, file_path: str, is_am: bool) -> Dict[str, Dict]:
        """Process a single SMAP granule file with enhanced validation"""
        data = {}
        
        try:
            with h5py.File(file_path, 'r') as f:
                # Select paths based on AM/PM
                if is_am:
                    base_path = 'Soil_Moisture_Retrieval_Data_AM'
                    sm_path = 'soil_moisture'
                    qual_path = 'retrieval_qual_flag'
                    lat_path = 'latitude'
                    lon_path = 'longitude'
                else:
                    base_path = 'Soil_Moisture_Retrieval_Data_PM'
                    sm_path = 'soil_moisture_dca_pm'
                    qual_path = 'retrieval_qual_flag_dca_pm'
                    lat_path = 'latitude_pm'
                    lon_path = 'longitude_pm'
                
                paths = {
                    'soil_moisture': f'{base_path}/{sm_path}',
                    'retrieval_qual_flag': f'{base_path}/{qual_path}',
                    'latitude': f'{base_path}/{lat_path}',
                    'longitude': f'{base_path}/{lon_path}'
                }
                
                # Load and validate datasets
                datasets = {}
                for key, path in paths.items():
                    if path not in f:
                        logger.error(f"Missing dataset: {path}")
                        # Print available paths
                        logger.info("Available paths:")
                        f.visit(lambda x: logger.info(x))
                        return data
                        
                    datasets[key] = f[path][:]
                
                # Validate data ranges
                sm_valid = np.sum(datasets['soil_moisture'] != -9999.0)
                total_pixels = datasets['soil_moisture'].size
                logger.info(
                    f"Valid soil moisture pixels: {sm_valid}/{total_pixels} "
                    f"({sm_valid/total_pixels*100:.1f}%)"
                )
                
                # Process each station with more validation
                for station in self.stations:
                    try:
                        result = self._get_station_data(
                            datasets['soil_moisture'],
                            datasets['retrieval_qual_flag'],
                            datasets['latitude'],
                            datasets['longitude'],
                            station.latitude,
                            station.longitude
                        )
                        
                        if result is not None:
                            sm_value, quality_flag = result
                            if not np.isnan(sm_value):
                                data[station.id] = {
                                    'soil_moisture': float(sm_value),
                                    'quality_flag': quality_flag,
                                    'is_am': is_am
                                }
                                logger.info(
                                    f"Station {station.id}: "
                                    f"moisture={sm_value:.3f}, "
                                    f"quality={quality_flag}"
                                )
                    except Exception as e:
                        logger.error(f"Error processing station {station.id}: {e}")
                        
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            
        return data


    def _get_station_data_chunked(self, sm, quality, lat, lon, target_lat, target_lon):
        """Process station data in chunks with stable distance calculation"""
        n_points = sm.size
        n_chunks = (n_points + self.chunk_size - 1) // self.chunk_size
        
        # Initialize accumulators
        weighted_sum = 0.0
        weight_sum = 0.0
        quality_sum = 0.0
        
        for i in range(n_chunks):
            start_idx = i * self.chunk_size
            end_idx = min((i + 1) * self.chunk_size, n_points)
            chunk_slice = slice(start_idx, end_idx)
            
            # Get chunk data
            lat_chunk = lat.flat[chunk_slice]
            lon_chunk = lon.flat[chunk_slice]
            sm_chunk = sm.flat[chunk_slice]
            quality_chunk = quality.flat[chunk_slice]
            
            # Convert to radians
            lat1 = np.deg2rad(target_lat)
            lon1 = np.deg2rad(target_lon)
            lat2 = np.deg2rad(lat_chunk)
            lon2 = np.deg2rad(lon_chunk)
            
            # Haversine formula components
            dlat = np.abs(lat2 - lat1)
            dlon = np.abs(lon2 - lon1)
            
            # Stable haversine formula implementation
            a = np.sin(dlat/2)**2
            b = np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
            c = np.clip(a + b, 0, 1)  # Ensure values stay in valid range
            
            distances = 2 * 6371 * np.arcsin(np.sqrt(c))
            
            # Apply radius filter
            mask = distances <= self.radius_km
            if not np.any(mask):
                continue
                
            # Calculate weights
            weights = 1 / (distances[mask] + 0.1)
            quality_weights = np.where(quality_chunk[mask] == 0, 1.0, 0.5)
            final_weights = weights * quality_weights
            
            # Update accumulators
            weighted_sum += np.sum(sm_chunk[mask] * final_weights)
            weight_sum += np.sum(final_weights)
            quality_sum += np.sum((quality_chunk[mask] == 0) * weights)
        
        if weight_sum == 0:
            return np.nan, 0.0
            
        sm_value = weighted_sum / weight_sum
        quality_score = (quality_sum / weight_sum) * 100  # Fixed weight normalization
        
        return self._normalize_soil_moisture(sm_value), quality_score


    def _get_watershed_mask(self, lats, lons, station):
        """Create mask for pixels within station's watershed"""
        if self.watersheds is None:
            return None
            
        try:
            # Find watershed for this station
            watershed = self.watersheds[self.watersheds['station_id'] == station.id]
            if watershed.empty:
                return None
                
            # Process in chunks to save memory
            mask = np.zeros(lats.shape, dtype=bool)
            
            for i in range(0, lats.size, self.chunk_size):
                chunk_slice = slice(i, min(i + self.chunk_size, lats.size))
                
                # Get chunk coordinates
                lats_chunk = lats.flat[chunk_slice]
                lons_chunk = lons.flat[chunk_slice]
                
                # Check containment
                from shapely.geometry import Point
                points = [Point(lon, lat) for lon, lat in zip(lons_chunk, lats_chunk)]
                chunk_mask = [watershed.geometry.contains(point).any() for point in points]
                
                # Update main mask
                mask.flat[chunk_slice] = chunk_mask
                
            return mask
            
        except Exception as e:
            logger.error(f"Error creating watershed mask: {e}")
            return None
        
    def _get_station_data_watershed(self, sm: np.ndarray, quality: np.ndarray, 
                                lat: np.ndarray, lon: np.ndarray, 
                                watershed_shape) -> Tuple[float, float]:
        """
        TODO: Future implementation for watershed-based soil moisture calculation.
        Will replace _get_station_data when ready.
        
        Args:
            watershed_shape: Will be shapely.geometry object representing watershed boundary
        """
        # This will be implemented when moving to watershed shapes
        raise NotImplementedError("Watershed-based calculation not yet implemented")
    


    ''''
Check L4 (source=1)
↓
If quality_flag good:
   Use L4 value
Else:
   Check L3 value
   If L3 quality_flag good:
      Use L3 value
   Else:
      Use L4 despite poor quality
    '''