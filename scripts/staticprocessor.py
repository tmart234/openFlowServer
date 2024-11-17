import sqlite3
import logging
import zipfile
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import earthaccess
import rasterio
from rasterio.windows import Window

from stations import Station

logger = logging.getLogger(__name__)

class StaticProcessor:
    """Process and store static features for stations"""
    
    def __init__(self, stations: List[Station]):
        self.stations = stations
        
        # Initialize auth
        try:
            earthaccess.login(strategy="environment")
            logger.info("Authentication successful")
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise

        logger.info(f"Initialized Static Processor with {len(stations)} stations")
        
        # Process both types of static data
        elevation_data = self._process_elevation()
        soil_data = self._process_soil_properties()
        
        # Combine and save all static features
        self._save_combined_data(elevation_data, soil_data)

    def _process_elevation(self) -> Dict:
        """Process SRTM elevation data and calculate slope"""
        elevation_data = {}
        temp_dir = Path("data/temp_static")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Get bounding coordinates
            lats = [s.latitude for s in self.stations]
            lons = [s.longitude for s in self.stations]
            
            # Search for SRTM data with proper bounding box format
            granules = earthaccess.search_data(
                short_name="SRTMGL1",
                provider="LPCLOUD",
                bounding_box=(
                    min(lons),       # lower_left_lon
                    min(lats),       # lower_left_lat
                    max(lons),       # upper_right_lon
                    max(lats)        # upper_right_lat
                )
            )
            
            if not granules:
                logger.warning("No SRTM data found")
                return elevation_data

            for station in self.stations:
                try:
                    elevation, slope = self._get_elevation_and_slope(
                        granules, temp_dir, station.latitude, station.longitude
                    )
                    elevation_data[station.id] = {
                        'elevation': elevation,
                        'slope': slope
                    }
                    logger.info(f"Processed elevation data for {station.id}: {elevation}m, slope: {slope}°")
                except Exception as e:
                    logger.error(f"Error processing elevation for station {station.id}: {e}")
                    
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
                
        return elevation_data



    def _process_soil_properties(self) -> Dict:
        """Process IsricWise soil data"""
        soil_data = {}
        temp_dir = Path("data/temp_static")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Search for ISRIC-WISE data
            granules = earthaccess.search_data(
                short_name="IsricWiseGrids_546",
                provider="ORNL_CLOUD"
            )
            
            if not granules:
                logger.warning("No ISRIC-WISE soil data found")
                return soil_data

            # Initialize default values for all stations
            for station in self.stations:
                soil_data[station.id] = {
                    'soil_type': 'UNKNOWN',
                    'soil_texture': 'UNKNOWN',
                    'organic_carbon': 0.0,
                    'clay_content': 0.0,
                    'sand_content': 0.0
                }

            # Process each file one at a time
            processed_files = {}
            for granule in granules:
                try:
                    downloaded = earthaccess.download(granule, local_path=str(temp_dir))
                    if not downloaded or downloaded[0].endswith('.sha256'):
                        continue
                    
                    file_path = Path(downloaded[0])
                    file_name = file_path.name
                    logger.info(f"Processing {file_name}")

                    # Check each file type and process accordingly
                    if 'wise_sc1.dat' in file_name:
                        for station in self.stations:
                            value = self._get_soil_class(file_path, station.latitude, station.longitude)
                            if value != "UNKNOWN":
                                soil_data[station.id]['soil_type'] = value
                        processed_files['sc1'] = True

                    elif 'wise_sc2.dat' in file_name:
                        for station in self.stations:
                            value = self._get_soil_class(file_path, station.latitude, station.longitude)
                            if value != "UNKNOWN":
                                soil_data[station.id]['soil_texture'] = value
                        processed_files['sc2'] = True

                    elif 'wise_cac.dat' in file_name:
                        for station in self.stations:
                            value = self._get_soil_value(file_path, station.latitude, station.longitude)
                            if value > 0:
                                soil_data[station.id]['clay_content'] = value
                        processed_files['cac'] = True

                    elif 'wise_ph1.dat' in file_name:
                        for station in self.stations:
                            ph_value = self._get_soil_value(file_path, station.latitude, station.longitude)
                            if ph_value > 0:
                                # Estimate organic carbon based on pH
                                soil_data[station.id]['organic_carbon'] = max(0, (7 - ph_value) * 2)
                        processed_files['ph1'] = True

                    elif 'wise_awc.dat' in file_name:
                        for station in self.stations:
                            awc_value = self._get_soil_value(file_path, station.latitude, station.longitude)
                            clay = soil_data[station.id].get('clay_content', 0)
                            if awc_value > 0:
                                # Estimate sand content based on AWC and clay content
                                soil_data[station.id]['sand_content'] = max(0, 100 - clay - awc_value * 10)
                        processed_files['awc'] = True

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                finally:
                    try:
                        file_path.unlink(missing_ok=True)
                    except Exception as e:
                        logger.error(f"Error removing file {file_path}: {e}")

            # Log what we processed
            logger.info(f"Processed soil files: {', '.join(processed_files.keys())}")
            
            # Validate the data
            for station_id, data in soil_data.items():
                logger.info(f"Station {station_id} soil data:")
                logger.info(f"  Soil Type: {data['soil_type']}")
                logger.info(f"  Soil Texture: {data['soil_texture']}")
                logger.info(f"  Clay Content: {data['clay_content']:.1f}%")
                logger.info(f"  Organic Carbon: {data['organic_carbon']:.1f}%")
                logger.info(f"  Sand Content: {data['sand_content']:.1f}%")
            
            return soil_data
                        
        finally:
            # Final cleanup of temp directory
            for file in temp_dir.glob('*'):
                try:
                    file.unlink()
                except Exception as e:
                    logger.error(f"Error removing file {file}: {e}")
            try:
                temp_dir.rmdir()
            except Exception as e:
                logger.error(f"Error cleaning temp directory: {e}")

    def _get_soil_value(self, file_path: Path, lat: float, lon: float) -> float:
        """Extract numeric value from soil data file for given coordinates"""
        try:
            with rasterio.open(file_path) as src:
                row, col = src.index(lon, lat)
                window = Window(col, row, 1, 1)
                data = src.read(1, window=window)
                if data.size > 0 and data[0,0] != -9999:
                    return float(data[0,0])
        except Exception as e:
            logger.error(f"Error getting soil value from {file_path}: {e}")
        return 0.0
    
    def _get_soil_class(self, file_path: Path, lat: float, lon: float) -> str:
        """Extract soil classification from data file"""
        try:
            value = self._get_soil_value(file_path, lat, lon)
            if value > 0:
                return f"Class_{int(value)}"
        except Exception as e:
            logger.error(f"Error getting soil class from {file_path}: {e}")
        return "UNKNOWN"

    def _extract_soil_class(self, file_path: Path, lat: float, lon: float) -> str:
        """Extract soil classification from IsricWise data file"""
        try:
            value = self._extract_value(file_path, lat, lon)
            # Map numeric value to soil class string (need IsricWise documentation)
            return f"Class_{int(value)}"
        except Exception as e:
            logger.error(f"Error extracting soil class: {e}")
            return "UNKNOWN"

    def _extract_value(self, file_path: Path, lat: float, lon: float) -> float:
        """Extract value from IsricWise data file for given coordinates"""
        try:
            with rasterio.open(file_path) as src:
                row, col = src.index(lon, lat)
                value = src.read(1, window=Window(col, row, 1, 1))
                return float(value[0,0])
        except Exception as e:
            logger.error(f"Error extracting value: {e}")
            return 0.0

    def _get_soil_properties(self, granules, temp_dir, lat: float, lon: float) -> Dict:
        """Get soil properties for a location"""
        soil_props = {
            'soil_type': 'UNKNOWN',
            'soil_texture': 'UNKNOWN',
            'organic_carbon': 0.0,
            'clay_content': 0.0,
            'sand_content': 0.0
        }
        
        for granule in granules:
            downloaded = earthaccess.download(granule, local_path=str(temp_dir))
            if not downloaded:
                continue
                
            file_path = downloaded[0]
            try:
                # Process based on file type
                if 'sc' in file_path.lower():
                    soil_props['soil_type'] = self._extract_soil_class(file_path, lat, lon)
                elif 'cac' in file_path.lower():
                    soil_props['clay_content'] = self._extract_value(file_path, lat, lon)
                elif 'sand' in file_path.lower():
                    soil_props['sand_content'] = self._extract_value(file_path, lat, lon)
                elif 'oc' in file_path.lower():
                    soil_props['organic_carbon'] = self._extract_value(file_path, lat, lon)
                    
            except Exception as e:
                logger.error(f"Error processing soil file {file_path}: {e}")
            finally:
                Path(file_path).unlink(missing_ok=True)
                
        return soil_props

    def _get_bounding_box(self) -> List[float]:
        """Get bounding box covering all stations"""
        lats = [s.latitude for s in self.stations]
        lons = [s.longitude for s in self.stations]
        return [
            min(lons) - 0.1,  # Add buffer
            min(lats) - 0.1,
            max(lons) + 0.1,
            max(lats) + 0.1
        ]

    def _save_combined_data(self, elevation_data: Dict, soil_data: Dict):
        """Update stations table with static features"""
        try:
            with sqlite3.connect("data/earth_data.db") as conn:
                for station_id in set(elevation_data.keys()) | set(soil_data.keys()):
                    elev_data = elevation_data.get(station_id, {})
                    soil_props = soil_data.get(station_id, {})
                    
                    conn.execute('''
                        UPDATE stations 
                        SET elevation = ?,
                            slope = ?,
                            soil_type = ?,
                            soil_texture = ?,
                            organic_carbon = ?,
                            clay_content = ?,
                            sand_content = ?
                        WHERE id = ?
                    ''', (
                        elev_data.get('elevation', 0.0),
                        elev_data.get('slope', 0.0),
                        soil_props.get('soil_type', 'UNKNOWN'),
                        soil_props.get('soil_texture', 'UNKNOWN'),
                        soil_props.get('organic_carbon', 0.0),
                        soil_props.get('clay_content', 0.0),
                        soil_props.get('sand_content', 0.0),
                        station_id
                    ))
                conn.commit()
                logger.info(f"Updated static features for {len(set(elevation_data.keys()) | set(soil_data.keys()))} stations")
                
        except Exception as e:
            logger.error(f"Error saving static data: {e}")


    @classmethod
    def readout(cls, db_path: Path):
        """Display static data summary from database"""
        logger.info("\nStatic Data Summary")
        logger.info("-" * 50)
        
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute('''
                    SELECT id,
                        elevation,
                        slope,
                        soil_type,
                        soil_texture,
                        organic_carbon,
                        clay_content,
                        sand_content
                    FROM stations
                    ORDER BY id
                ''')
                
                rows = cursor.fetchall()
                if not rows:
                    logger.info("No static data found in database")
                    return
                    
                for row in rows:
                    logger.info(f"\nStation {row[0]}:")
                    logger.info(f"  Elevation: {row[1]:.1f} m")
                    logger.info(f"  Slope: {row[2]:.1f}°")
                    logger.info(f"  Soil Type: {row[3]}")
                    logger.info(f"  Soil Texture: {row[4]}")
                    logger.info(f"  Organic Carbon: {row[5]:.1f}%")
                    logger.info(f"  Clay Content: {row[6]:.1f}%")
                    logger.info(f"  Sand Content: {row[7]:.1f}%")
                
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")


    def _get_elevation_and_slope(self, granules, temp_dir, lat: float, lon: float) -> Tuple[float, float]:
        """Get elevation and calculate slope for a location"""
        for granule in granules:
            downloaded = earthaccess.download(granule, local_path=str(temp_dir))
            if not downloaded:
                continue
                
            file_path = downloaded[0]
            if file_path.endswith('.zip'):
                # Extract the HGT file
                with zipfile.ZipFile(file_path) as zf:
                    hgt_file = next(name for name in zf.namelist() if name.endswith('.hgt'))
                    zf.extract(hgt_file, temp_dir)
                    file_path = Path(temp_dir) / hgt_file

            try:
                with rasterio.open(file_path) as src:
                    # Convert lat/lon to pixel coordinates
                    row, col = src.index(lon, lat)
                    
                    # Make sure we have room for a 3x3 window
                    if row < 1 or row >= src.height-1 or col < 1 or col >= src.width-1:
                        continue

                    # Get elevation data
                    window = rasterio.windows.Window(col-1, row-1, 3, 3)
                    data = src.read(1, window=window)
                    
                    if data.size != 9:  # Must have complete 3x3 window
                        continue
                        
                    elevation = float(data[1,1])
                    
                    # Calculate slope using D8 method
                    cell_size = 30  # SRTM resolution in meters
                    dx = ((int(data[0,2]) + 2*int(data[1,2]) + int(data[2,2])) - 
                        (int(data[0,0]) + 2*int(data[1,0]) + int(data[2,0]))) / (8 * cell_size)
                    dy = ((int(data[2,0]) + 2*int(data[2,1]) + int(data[2,2])) - 
                        (int(data[0,0]) + 2*int(data[0,1]) + int(data[0,2]))) / (8 * cell_size)
                        
                    slope = np.degrees(np.arctan(np.sqrt(dx*dx + dy*dy)))
                    return elevation, slope
                    
            except Exception as e:
                logger.error(f"Error processing SRTM file {file_path}: {e}")
                continue
                
        return 0.0, 0.0  # Default values if all processing fails