import sqlite3
import logging
from datetime import datetime
from typing import List
from pathlib import Path

from stations import Station

logger = logging.getLogger(__name__)


def check_database_structure(conn: sqlite3.Connection) -> bool:
    """Check if database has all required tables with correct schema"""
    expected_tables = {
        'stations': ['id', 'source', 'site_id', 'latitude', 'longitude', 'created_at',
                    'elevation', 'slope', 'soil_type', 'soil_texture', 
                    'organic_carbon', 'clay_content', 'sand_content'],
        'smap_features': ['timestamp', 'station_id', 'soil_moisture', 'quality_flag', 'trend3', 'source'],
        'vegetation_features': ['timestamp', 'station_id', 'ndvi', 'quality_score'],
        'snow_features': ['timestamp', 'station_id', 'snow_cover', 'quality_score']
    }
    
    try:
        for table, expected_columns in expected_tables.items():
            cursor = conn.execute(f"PRAGMA table_info({table})")
            existing_columns = [row[1] for row in cursor.fetchall()]
            
            if not existing_columns:
                logger.warning(f"Missing table: {table}")
                return False
                
            missing_columns = set(expected_columns) - set(existing_columns)
            if missing_columns:
                logger.warning(f"Missing columns in {table}: {missing_columns}")
                return False
                
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Error checking database structure: {e}")
        return False

def setup_database(db_path: Path):
    """Initialize SQLite database tables if they don't exist or verify structure"""
    exists = db_path.exists()
    logger.info(f"{'Checking' if exists else 'Creating'} database at {db_path}")
    
    with sqlite3.connect(db_path) as conn:
        if exists:
            if check_database_structure(conn):
                logger.info("Database structure verified")
                return
            else:
                logger.warning("Database exists but structure is incorrect. Dropping and recreating tables...")
                # Drop existing tables in correct order (due to foreign keys)
                conn.execute("DROP TABLE IF EXISTS smap_features")
                conn.execute("DROP TABLE IF EXISTS vegetation_features")
                conn.execute("DROP TABLE IF EXISTS snow_features")
                conn.execute("DROP TABLE IF EXISTS static_features")
                conn.execute("DROP TABLE IF EXISTS stations")
        
        # Create combined stations table with static features
        conn.execute('''
            CREATE TABLE IF NOT EXISTS stations (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                site_id TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                created_at INTEGER NOT NULL,
                elevation REAL DEFAULT 0.0,          -- Elevation in meters
                slope REAL DEFAULT 0.0,              -- Terrain slope
                soil_type TEXT DEFAULT 'UNKNOWN',    -- Soil classification
                soil_texture TEXT DEFAULT 'UNKNOWN', -- Soil texture class
                organic_carbon REAL DEFAULT 0.0,     -- Soil organic carbon content
                clay_content REAL DEFAULT 0.0,       -- Clay percentage
                sand_content REAL DEFAULT 0.0        -- Sand percentage
            )
        ''')
        
        # Create soil moisture features table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS smap_features (
                timestamp INTEGER,
                station_id TEXT,
                soil_moisture REAL,      -- Normalized soil moisture (0-1)
                quality_flag INTEGER,    -- Original SMAP quality flag (0-1)
                trend3 REAL,             -- 3-day trend
                source INTEGER,          -- Binary: 0=L3, 1=L4
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
        
        logger.info("Database tables created successfully")


def store_stations(stations: List[Station], db_path: Path):
    """Store basic station information in the database"""
    current_time = int(datetime.now().timestamp())
    
    with sqlite3.connect(db_path) as conn:
        for station in stations:
            source, site_id = station.id.split(':')
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO stations 
                    (id, source, site_id, latitude, longitude, created_at,
                     elevation, slope, soil_type, soil_texture, 
                     organic_carbon, clay_content, sand_content)
                    VALUES (?, ?, ?, ?, ?, ?, 0.0, 0.0, 'UNKNOWN', 'UNKNOWN', 0.0, 0.0, 0.0)
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