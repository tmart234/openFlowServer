import logging
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger(__name__)

@dataclass
class Station:
    """Single station location data"""
    id: str
    latitude: float
    longitude: float

def get_usgs_coordinates(site_number: str) -> Optional[Dict]:
    """Get coordinates for a USGS station"""
    base_url = "https://waterdata.usgs.gov/nwis/inventory"
    params = {
        'search_site_no': site_number,
        'search_site_no_match_type': 'exact',
        'group_key': 'NONE',
        'format': 'sitefile_output',
        'sitefile_output_format': 'rdb',
        'column_name': 'site_no,station_nm,dec_lat_va,dec_long_va',
        'list_of_search_criteria': 'search_site_no'
    }
    
    import requests
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
    
    import requests
    full_url = f"{base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
    logger.info(f"Fetching DWR coordinates - URL: {full_url}")
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
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
        
    except Exception as e:
        logger.error(f"Error getting DWR coordinates for {abbrev}: {e}")
        return None