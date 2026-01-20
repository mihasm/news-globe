// src/server.rs
//
// Minimal HTTP server for geodb.
// - Loads DB into RAM once (Db bytes + fst::Map).
// - Serves GET /query?key=...&limit=...
// - Optionally /health
//
// Uses axum + tokio. No unsafe.

use anyhow::{anyhow, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use fst;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use crate::{open_db, read_postings, read_record_by_id, Db};

#[derive(Clone)]
pub struct AppState {
    db: Arc<Db>,
    fst: Arc<fst::Map<Vec<u8>>>,
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    key: String,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct OutCandidateOwned {
    geoname_id: u32,
    name: String,
    country: String,
    admin1: String,
    admin2: String,
    lat: f32,
    lon: f32,
    feature_class: char,
    feature_code: String,
    population: u32,
}

#[derive(Serialize)]
struct OutJsonOwned {
    key: String,
    count: usize,
    candidates: Vec<OutCandidateOwned>,
}

#[derive(Serialize)]
struct ErrorJson {
    error: String,
}

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let msg = format!("{:#}", self.0);
        let body = Json(ErrorJson { error: msg });

        // Basic mapping; adjust if you want.
        let status = StatusCode::BAD_REQUEST;
        (status, body).into_response()
    }
}

pub async fn serve(db_path: PathBuf, bind: SocketAddr) -> Result<()> {
    let db = open_db(&db_path)?;
    let fst_map = fst::Map::new(db.fst_slice().to_vec()).map_err(|e| anyhow!("fst load: {e}"))?;

    let state = AppState {
        db: Arc::new(db),
        fst: Arc::new(fst_map),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/query", get(query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn query(
    State(state): State<AppState>,
    Query(q): Query<QueryParams>,
) -> Result<impl IntoResponse, AppError> {
    let lookup_key = q.key.trim().to_lowercase();
    let limit = q.limit.unwrap_or(0);

    // Keep allocations tight.
    let mut candidates: Vec<OutCandidateOwned> = Vec::new();

    if let Some(off) = state.fst.get(&lookup_key) {
        let mut ids = read_postings(&state.db, off as usize).map_err(AppError)?;

        if limit != 0 && ids.len() > limit {
            ids.truncate(limit);
        }

        for id in ids {
            if let Some(rec) = read_record_by_id(&state.db, id).map_err(AppError)? {
                candidates.push(OutCandidateOwned {
                    geoname_id: rec.id,
                    name: rec.name,
                    country: rec.country,
                    admin1: rec.admin1,
                    admin2: rec.admin2,
                    lat: rec.lat,
                    lon: rec.lon,
                    feature_class: rec.feat_class as char,
                    feature_code: rec.feat_code,
                    population: rec.population,
                });
            }
        }
    }

    let out = OutJsonOwned {
        key: q.key,
        count: candidates.len(),
        candidates,
    };

    Ok((StatusCode::OK, Json(out)))
}