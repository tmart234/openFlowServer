use actix_web::{web, App, ResponseError, HttpServer, HttpResponse, middleware::Logger, http::StatusCode};
use serde::{Deserialize, Serialize};
use chrono::NaiveDate;
use std::sync::{Mutex, Arc};
use log::{info, error};
use env_logger::Env;
use thiserror::Error;
use governor::{state::DashMap, RateLimiter, Quota};
use std::num::NonZeroU32;
use reqwest;
use hdf5::{File, types::Array};
use std::time::Duration;
use tokio::time;
use tempfile::NamedTempFile;
use std::io::copy;
use rusqlite::{params, Connection, Result as SqliteResult};
use rusqlite::types::{FromSql, ValueRef, FromSqlResult};
use rusqlite::{ToSql, types::ToSqlOutput};
use rayon::prelude::*;
use tuf::crypto::KeyId;
use tuf::client::{Client, Config};
use tuf::metadata::{RootMetadata, SignedMetadata, Role, MetadataPath, MetadataVersion};
use tuf::interchange::DataInterchange;
use tuf::repository::{FileSystemRepository, HttpRepository};
use url::Url;
use reqwest::blocking::Client as HttpClient;

// Define error types
#[derive(Error, Debug)]
enum ApiError {
    #[error("Internal server error")]
    InternalServerError,
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("SMAP data download failed: {0}")]
    SmapDownloadError(String),
    #[error("Database error: {0}")]
    DatabaseError(#[from] rusqlite::Error),
    #[error("TUF update failed: {0}")]
    TufUpdateError(String),
}

impl FromSql for NaiveDate {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let string = value.as_str()?;
        Ok(NaiveDate::from_str(string).unwrap())
    }
}

impl ToSql for NaiveDate {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

// Define app state
struct AppState {
    db: Mutex<Connection>,
    rate_limiter: Arc<RateLimiter<String, DashMap<String, u64>, governor::clock::DefaultClock>>,
}

// Define soil moisture data struct
#[derive(Serialize, Deserialize, Clone, Debug)]
struct SoilMoistureData {
    date: NaiveDate,
    lat: f64,
    lon: f64,
    moisture: f64,
}

// Define moisture query struct
#[derive(Deserialize)]
struct MoistureQuery {
    lat: f64,
    lon: f64,
    start_date: NaiveDate,
    end_date: NaiveDate,
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::RateLimitExceeded => StatusCode::TOO_MANY_REQUESTS,
            ApiError::InvalidInput(_) => StatusCode::BAD_REQUEST,
            ApiError::SmapDownloadError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::TufUpdateError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// Define API routes
async fn get_soil_moisture(
    query: web::Query<MoistureQuery>,
    data: web::Data<AppState>,
    client_ip: web::Header<actix_web::http::header::HeaderValue>,
) -> Result<HttpResponse, ApiError> {
    // Rate limiting
    let key = client_ip.to_str()?;
    if data.rate_limiter.check_key(&key).is_err() {
        return Err(ApiError::RateLimitExceeded);
    }

    // Database query
    let db = data.db.lock().map_err(|_| ApiError::InternalServerError)?;
    let mut stmt = db.prepare("
        SELECT date, lat, lon, moisture 
        FROM soil_moisture 
        WHERE lat = ?1 AND lon = ?2 AND date BETWEEN ?3 AND ?4
    ")?;

    let results: SqliteResult<Vec<SoilMoistureData>> = stmt.query_map(
        params![query.lat, query.lon, query.start_date.to_string(), query.end_date.to_string()],
        |row| Ok(SoilMoistureData {
            date: NaiveDate::from_str(row.get(0)?).unwrap(),
            lat: row.get(1)?,
            lon: row.get(2)?,
            moisture: row.get(3)?,
        })
    )?.collect();

    match results {
        Ok(data) => Ok(HttpResponse::Ok().json(data)),
        Err(e) => {
            error!("Database error: {:?}", e);
            Err(ApiError::DatabaseError(e))
        }
    }
}

async fn update_smap_data(data: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    // Download and process SMAP data
    let new_data = download_and_process_smap_data().await?;
    let db = data.db.lock().map_err(|_| ApiError::InternalServerError)?;
    
    // Insert data into database
    db.execute("BEGIN TRANSACTION", params![])?;
    let mut stmt = db.prepare("
        INSERT OR REPLACE INTO soil_moisture (date, lat, lon, moisture)
        VALUES (?, ?, ?, ?)
    ")?;

    for item in new_data {
        stmt.execute(params![item.date.to_string(), item.lat, item.lon, item.moisture])?;
    }

    db.execute("COMMIT", params![])?;
    Ok(HttpResponse::Ok().body("SMAP data updated"))
}

async fn download_and_process_smap_data() -> Result<Vec<SoilMoistureData>, ApiError> {
    // SMAP data URL (you'll need to replace this with the actual URL for the dataset you need)
    let smap_url = "https://n5eil01u.ecs.nsidc.org/SMAP/SPL3SMP.007/2024.07.29/SMAP_L3_SM_P_20240729_R18290_001.h5";

    // Download the file
    let client = reqwest::Client::new();
    let response = client.get(smap_url)
        .send()
        .await
        .map_err(|e| ApiError::SmapDownloadError(e.to_string()))?;

    if !response.status().is_success() {
        return Err(ApiError::SmapDownloadError("Failed to download SMAP data".to_string()));
    }

    // Save the downloaded content to a temporary file
    let mut temp_file = NamedTempFile::new().map_err(|e| ApiError::SmapDownloadError(e.to_string()))?;
    copy(&mut response.bytes().await.unwrap().as_ref(), &mut temp_file).map_err(|e| ApiError::SmapDownloadError(e.to_string()))?;

    // Process the HDF5 file
    process_smap_data(temp_file.path().to_str().unwrap(), 1000)
        .map_err(|e| ApiError::SmapDownloadError(e.to_string()))
}

fn process_smap_data(file_path: &str, chunk_size: usize) -> Result<Vec<SoilMoistureData>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    
    let soil_moisture = file.dataset("Soil_Moisture_Retrieval_Data/soil_moisture")?;
    let latitudes = file.dataset("Soil_Moisture_Retrieval_Data/latitude")?;
    let longitudes = file.dataset("Soil_Moisture_Retrieval_Data/longitude")?;
        
    let total_size = soil_moisture.size();
    let num_chunks = (total_size + chunk_size - 1) / chunk_size;
        
    let result = Mutex::new(Vec::new());
        
    (0..num_chunks).into_par_iter().try_for_each(|i| {
        let start = i * chunk_size;
        let end = std::cmp::min((i + 1) * chunk_size, total_size);
        
        let moisture_chunk: Array<f32, _> = soil_moisture.read_slice_1d(start..end)?;
        let moisture_vec: Vec<f32> = moisture_chunk.try_into().map_err(|e| {
            error!("Error converting Array to Vec: {:?}", e);
            e
        })?;
        
        let lat_chunk: Vec<f32> = latitudes.read_slice_1d(start..end)?;
        let lon_chunk: Vec<f32> = longitudes.read_slice_1d(start..end)?;
        
        let chunk_data: Vec<SoilMoistureData> = moisture_vec.into_iter()
            .zip(lat_chunk.into_iter().zip(lon_chunk.into_iter()))
            .map(|(moisture, (lat, lon))| SoilMoistureData {
                date: chrono::NaiveDate::from_ymd_opt(2024, 7, 29).unwrap(), // Example date
                lat: lat as f64,
                lon: lon as f64,
                moisture: moisture as f64,
            })
            .collect();
        
        result.lock().unwrap().extend(chunk_data);
        Ok(())
    })?;
        
    Ok(result.into_inner().unwrap())
    }    
    
async fn update_tuf_data() -> Result<(), ApiError> {
    // TUF update logic goes here
    info!("Updating TUF data");
    // Add TUF update logic here
    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logger
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    // Initialize SQLite database
    let conn = Connection::open("soil_moisture.db").expect("Failed to open database");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS soil_moisture (
            date TEXT,
            lat REAL,
            lon REAL,
            moisture REAL,
            PRIMARY KEY (date, lat, lon)
        )",
        params![],
    ).expect("Failed to create table");

    // Initialize rate limiter: 20 requests per minute
    let rate_limiter = Arc::new(RateLimiter::keyed(Quota::per_minute(NonZeroU32::new(20).unwrap())));

    let app_state = web::Data::new(AppState {
        db: Mutex::new(conn),
        rate_limiter: rate_limiter.clone(),
    });

    // Spawn a task to update SMAP data daily
    let app_state_clone = app_state.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(24 * 60 * 60));
        loop {
            interval.tick().await;
            match update_smap_data(app_state_clone.clone()).await {
                Ok(_) => info!("SMAP data updated successfully"),
                Err(e) => error!("Failed to update SMAP data: {}", e),
            }
        }
    });

    // Spawn a task to update TUF data daily
    let app_state_clone = app_state.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(24 * 60 * 60));
        loop {
            interval.tick().await;
            match update_tuf_data().await {
                Ok(_) => info!("TUF data updated successfully"),
                Err(e) => error!("Failed to update TUF data: {}", e),
            }
        }
    });

    info!("Starting server at http://127.0.0.1:8080");

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(Logger::default())
            .route("/soil_moisture", web::get().to(get_soil_moisture))
            .route("/update_smap", web::post().to(update_smap_data))
            .route("/update_tuf", web::post().to(update_tuf_data))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}