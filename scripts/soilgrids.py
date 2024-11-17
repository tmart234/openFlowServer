import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from owslib.wcs import WebCoverageService
import requests
import numpy as np
from dataclasses import dataclass
import time

from stations import Station

logger = logging.getLogger(__name__)

@dataclass
class SoilGridsLayer:
    """Configuration for a SoilGrids layer"""
    name: str          # Name as it appears in REST API
    property_id: str   # Property ID for API
    units: str
    scaling_factor: float
    description: str

class SoilGridsProcessor:
    BASE_URL = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    
    # Update layer configurations for REST API
    LAYERS = {
        'clay': SoilGridsLayer(
            name='clay',
            property_id='clay',
            units='percent',
            scaling_factor=0.1,
            description='Clay content percentage'
        ),
        'sand': SoilGridsLayer(
            name='sand',
            property_id='sand',
            units='percent',
            scaling_factor=0.1,
            description='Sand content percentage'
        ),
        'organic_carbon': SoilGridsLayer(
            name='soc',
            property_id='soc',
            units='g/kg',
            scaling_factor=0.1,
            description='Organic carbon content'
        ),
        'bulk_density': SoilGridsLayer(
            name='bdod',
            property_id='bdod',
            units='kg/dm3',
            scaling_factor=0.01,
            description='Bulk density'
        )
    }
    
    def __init__(self, stations: List["Station"]):
        self.stations = stations
        self.temp_dir = Path("data/temp_soil")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized SoilGrids Processor with {len(stations)} stations")
    
    def _get_coordinate_subsets(self, lat: float, lon: float, buffer: float = 0.00001) -> List[Tuple]:
        """Get coordinate subsets for WCS 2.0 with minimal buffer"""
        return [
            ('X', f"{lon:.6f}", f"{lon:.6f}"),  # Exact point
            ('Y', f"{lat:.6f}", f"{lat:.6f}")   # Exact point
        ]


    def _get_layer_value(self, layer: SoilGridsLayer, lat: float, lon: float) -> Optional[float]:
        """Get value for a specific layer using REST API point query"""
        try:
            params = {
                'lat': round(float(lat), 6),
                'lon': round(float(lon), 6),
                'property': [layer.property_id],
                'depth': ['0-5cm'],
                'value': ['mean']
            }
            
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'error' in data:
                    logger.error(f"API error: {data['error']}")
                    return None
                    
                if not data.get('properties', {}).get('layers'):
                    logger.error(f"No data available at coordinates: lat={lat}, lon={lon}")
                    return None
                
                try:
                    value = data['properties']['layers'][0]['depths'][0]['values']['mean']
                    return float(value) * layer.scaling_factor
                except (KeyError, IndexError, TypeError) as e:
                    logger.error(f"Unexpected response structure: {data}")
                    return None
                    
            elif response.status_code == 429:
                logger.error("Rate limit exceeded")
                time.sleep(15)  # Wait for rate limit
                return None
                
            else:
                logger.error(f"HTTP error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            return None


    def process_soil_properties(self) -> Dict:
        """Process soil properties for all stations with rate limiting"""
        soil_data = {}
        
        for station in self.stations:
            try:
                station_data = {}
                logger.info(f"Processing soil data for station {station.id}")
                
                for layer_id, layer in self.LAYERS.items():
                    # Add delay between requests
                    if soil_data:  # Not first request
                        time.sleep(15)  # Respect rate limit
                        
                    value = self._get_layer_value(layer, station.latitude, station.longitude)
                    if value is not None:
                        station_data[layer_id] = value
                        logger.info(f"Got {layer_id}: {value} {layer.units} for {station.id}")
                    else:
                        logger.warning(f"No valid {layer_id} data for station {station.id}")
                
                soil_data[station.id] = station_data
                
            except Exception as e:
                logger.error(f"Error processing station {station.id}: {e}")
                
        return soil_data

    def cleanup(self):
        """Clean up temporary files"""
        try:
            for file in self.temp_dir.glob('*.tif'):
                file.unlink()
            self.temp_dir.rmdir()
        except Exception as e:
            logger.error(f"Error cleaning up temporary files: {e}")

    @classmethod
    def readout(cls, soil_data: Dict):
        """Display soil data summary"""
        logger.info("\nSoil Properties Summary")
        logger.info("-" * 50)
        
        for station_id, data in soil_data.items():
            logger.info(f"\nStation {station_id}:")
            for prop, value in data.items():
                if prop in cls.LAYERS:
                    logger.info(f"  {cls.LAYERS[prop].description}: {value:.1f} {cls.LAYERS[prop].units}")
                else:
                    logger.info(f"  {prop}: {value:.1f}")