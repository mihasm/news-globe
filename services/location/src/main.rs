// src/main.rs
// CLI + query-only code. No unsafe. Exact-match query (no normalization, no tokenization).

use anyhow::{anyhow, bail, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use clap::{Parser, Subcommand};
use serde::Serialize;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

mod build;
use build::{GeoRecord, MAGIC, VERSION};

mod server;

#[derive(Parser)]
#[command(name = "geodb")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Build {
        #[arg(long)]
        all: PathBuf,
        #[arg(long)]
        alt: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 0)]
        min_pop: u32,
    },
    Query {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = 0)]
        limit: usize,
    },
    Serve {
        #[arg(long)]
        db: PathBuf,
        /// Bind address, e.g. 127.0.0.1:8787
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: SocketAddr,
    },
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build { all, alt, out, min_pop } => build::build_db(&all, &alt, &out, min_pop),
        Cmd::Query { db, key, limit } => {
            let json = query_exact(&db, &key, limit)?;
            println!("{}", serde_json::to_string_pretty(&json)?);
            Ok(())
        }
        Cmd::Serve { db, bind } => server::serve(db, bind).await,
    }
}

/* -------------------------
   DB reader
-------------------------- */

struct Db {
    fst_start: usize,
    fst_len: usize,
    postings_start: usize,
    records_start: usize,
    offsets_start: usize,
    postings_len: usize,
    records_len: usize,
    offsets_len: usize,
    bytes: Vec<u8>,
}

impl Db {
    fn fst_slice(&self) -> &[u8] {
        &self.bytes[self.fst_start..self.fst_start + self.fst_len]
    }
    fn postings_slice(&self) -> &[u8] {
        &self.bytes[self.postings_start..self.postings_start + self.postings_len]
    }
    fn records_slice(&self) -> &[u8] {
        &self.bytes[self.records_start..self.records_start + self.records_len]
    }
    fn offsets_slice(&self) -> &[u8] {
        &self.bytes[self.offsets_start..self.offsets_start + self.offsets_len]
    }
}

fn open_db(path: &Path) -> Result<Db> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;

    let mut cur = std::io::Cursor::new(&bytes[..]);

    let mut magic = [0u8; 7];
    cur.read_exact(&mut magic)?;
    if &magic != MAGIC {
        bail!("bad magic");
    }
    let ver = cur.read_u32::<LittleEndian>()?;
    if ver != VERSION {
        bail!("unsupported version {ver}");
    }

    let fst_len = cur.read_u64::<LittleEndian>()? as usize;
    let postings_len = cur.read_u64::<LittleEndian>()? as usize;
    let records_len = cur.read_u64::<LittleEndian>()? as usize;
    let offsets_len = cur.read_u64::<LittleEndian>()? as usize;

    let header_len = 7 + 4 + 8 * 4;
    let fst_start = header_len;
    let postings_start = fst_start + fst_len;
    let records_start = postings_start + postings_len;
    let offsets_start = records_start + records_len;

    if offsets_start + offsets_len > bytes.len() {
        bail!("corrupt file lengths");
    }

    Ok(Db {
        fst_start,
        fst_len,
        postings_start,
        records_start,
        offsets_start,
        postings_len,
        records_len,
        offsets_len,
        bytes,
    })
}

/* -------------------------
   exact lookup query
-------------------------- */

fn query_exact(db_path: &Path, key: &str, limit: usize) -> Result<OutJsonOwned> {
    let db = open_db(db_path)?;
    let fst = fst::Map::new(db.fst_slice()).map_err(|e| anyhow!("fst load: {e}"))?;

    let mut candidates: Vec<OutCandidateOwned> = Vec::new();

    let lookup_key = key.trim().to_lowercase();

    if let Some(off) = fst.get(&lookup_key) {
        let mut ids = read_postings(&db, off as usize)?;
        if limit != 0 && ids.len() > limit {
            ids.truncate(limit);
        }

        for id in ids {
            if let Some(rec) = read_record_by_id(&db, id)? {
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

    let count = candidates.len();
    Ok(OutJsonOwned {
        key: key.to_string(),
        count,
        candidates,
    })
}

/* -------------------------
   postings decode + record load
-------------------------- */

fn read_postings(db: &Db, postings_offset: usize) -> Result<Vec<u32>> {
    let blob = db.postings_slice();
    if postings_offset >= blob.len() {
        bail!("postings offset out of bounds");
    }
    let slice = &blob[postings_offset..];

    let (len, len_bytes) = read_var_u32(slice)?;
    let start = len_bytes;
    let end = start + len as usize;
    if end > slice.len() {
        bail!("postings length out of bounds");
    }
    Ok(decode_delta_varints(&slice[start..end]))
}

fn read_record_by_id(db: &Db, id: u32) -> Result<Option<GeoRecord>> {
    let slice = db.offsets_slice();
    let mut cur = std::io::Cursor::new(slice);

    let n = cur.read_u32::<LittleEndian>()? as usize;
    let ids_start = 4;
    let ids_end = ids_start + n * 4;
    let offs_start = ids_end;
    let offs_end = offs_start + n * 8;
    if offs_end > slice.len() {
        bail!("corrupt offsets");
    }

    let ids_bytes = &slice[ids_start..ids_end];

    // binary search
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let mid_id = read_u32_le_at(ids_bytes, mid * 4);
        if mid_id < id {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo >= n {
        return Ok(None);
    }
    let found_id = read_u32_le_at(ids_bytes, lo * 4);
    if found_id != id {
        return Ok(None);
    }

    let offs_bytes = &slice[offs_start..offs_end];
    let off = read_u64_le_at(offs_bytes, lo * 8) as usize;

    let rec_blob = db.records_slice();
    if off >= rec_blob.len() {
        bail!("record offset out of bounds");
    }
    let mut c = std::io::Cursor::new(&rec_blob[off..]);

    let rid = c.read_u32::<LittleEndian>()?;
    let lat = c.read_f32::<LittleEndian>()?;
    let lon = c.read_f32::<LittleEndian>()?;
    let pop = c.read_u32::<LittleEndian>()?;
    let mut fc = [0u8; 1];
    c.read_exact(&mut fc)?;

    let name = read_lp_str_cur(&mut c)?;
    let country = read_lp_str_cur(&mut c)?;
    let admin1 = read_lp_str_cur(&mut c)?;
    let admin2 = read_lp_str_cur(&mut c)?;
    let feat_code = read_lp_str_cur(&mut c)?;

    Ok(Some(GeoRecord {
        id: rid,
        name,
        ascii_name: String::new(),
        country,
        admin1,
        admin2,
        lat,
        lon,
        feat_class: fc[0],
        feat_code,
        population: pop,
    }))
}

fn read_lp_str_cur(cur: &mut std::io::Cursor<&[u8]>) -> Result<String> {
    let pos = cur.position() as usize;
    let buf = cur.get_ref();

    let (len, len_bytes) = read_var_u32(&buf[pos..])?;
    let start = pos + len_bytes;
    let end = start + len as usize;
    if end > buf.len() {
        bail!("string out of bounds");
    }

    let s = std::str::from_utf8(&buf[start..end])?.to_string();
    cur.set_position(end as u64);
    Ok(s)
}

/* -------------------------
   varint + delta decode
-------------------------- */

fn decode_delta_varints(bytes: &[u8]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut cur = 0u32;
    while i < bytes.len() {
        let (v, n) = match read_var_u32(&bytes[i..]) {
            Ok(x) => x,
            Err(_) => break,
        };
        i += n;
        cur = cur.wrapping_add(v);
        out.push(cur);
    }
    out
}

fn read_var_u32(buf: &[u8]) -> Result<(u32, usize)> {
    let mut v: u32 = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate().take(5) {
        let chunk = (b & 0x7F) as u32;
        v |= chunk << shift;
        if (b & 0x80) == 0 {
            return Ok((v, i + 1));
        }
        shift += 7;
    }
    Err(anyhow!("bad varint"))
}

/* -------------------------
   little-endian helpers
-------------------------- */

fn read_u32_le_at(b: &[u8], off: usize) -> u32 {
    let x = &b[off..off + 4];
    u32::from_le_bytes([x[0], x[1], x[2], x[3]])
}

fn read_u64_le_at(b: &[u8], off: usize) -> u64 {
    let x = &b[off..off + 8];
    u64::from_le_bytes([x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]])
}