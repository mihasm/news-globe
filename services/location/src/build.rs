// Replace these parts in src/build.rs (this is the full, fixed build pipeline).
// Key fixes vs previous:
// - Removed `Send` bounds from ZIP-stream readers (ZipFile is not Send).
// - Removed unused imports.
// - Still streams directly from ZIP members (no extract-to-disk).
// - Still case-insensitive (lowercased index keys) + min_pop filtering.
// - VERSION bumped to 2.

use anyhow::{anyhow, bail, Context, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use fst::MapBuilder;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use zip::ZipArchive;

// fast hashmaps
use ahash::RandomState;
use hashbrown::{HashMap, HashSet};
use smallvec::SmallVec;

pub const MAGIC: &[u8; 7] = b"GEODB1\0";
pub const VERSION: u32 = 2;

const CHUNK_LINES: usize = 200_000;
const ZIP_BUF_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct GeoRecord {
    pub id: u32,
    pub name: String,
    pub ascii_name: String,
    pub country: String,
    pub admin1: String,
    pub admin2: String,
    pub lat: f32,
    pub lon: f32,
    pub feat_class: u8,
    pub feat_code: String,
    pub population: u32,
}

struct Progress {
    label: &'static str,
    start: Instant,
    every: u64,
    last_printed: AtomicU64,
}
impl Progress {
    fn new(label: &'static str, every: u64) -> Self {
        Self {
            label,
            start: Instant::now(),
            every,
            last_printed: AtomicU64::new(u64::MAX),
        }
    }
    fn tick(&self, n: u64, extra: &str) {
        if n == 0 {
            return;
        }
        if self.every == 0 || (n % self.every != 0) {
            return;
        }
        let prev = self.last_printed.swap(n, Ordering::Relaxed);
        if prev == n {
            return;
        }
        eprintln!(
            "[{:<14}] {:>12}  t={:>7.2}s  {}",
            self.label,
            n,
            self.start.elapsed().as_secs_f64(),
            extra
        );
    }
    fn done(&self, n: u64, extra: &str) {
        eprintln!(
            "[{:<14}] {:>12}  t={:>7.2}s  DONE  {}",
            self.label,
            n,
            self.start.elapsed().as_secs_f64(),
            extra
        );
    }
}

// Convenience types
type FastBuildMap = HashMap<String, SmallVec<[u32; 2]>, RandomState>;
type FastIdSet = HashSet<u32, RandomState>;

#[inline]
fn norm_key(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_lowercase())
    }
}

/// Open a specific member from a ZIP and run a function over a buffered reader for that member.
/// Avoids extracting the uncompressed text to disk.
fn with_zip_member<Rv>(
    zip_path: &Path,
    member_name: &str,
    f: impl for<'a> FnOnce(BufReader<zip::read::ZipFile<'a>>) -> Result<Rv>,
) -> Result<Rv> {
    let file = File::open(zip_path)
        .with_context(|| format!("open zip: {}", zip_path.display()))?;
    let mut zip = ZipArchive::new(file)
        .with_context(|| format!("read zip: {}", zip_path.display()))?;

    let member = zip
        .by_name(member_name)
        .with_context(|| format!("file {member_name} not found in zip {}", zip_path.display()))?;

    let reader = BufReader::with_capacity(ZIP_BUF_BYTES, member);
    f(reader)
}

pub fn build_db(all_zip: &Path, alt_zip: &Path, out_db: &Path, min_pop: u32) -> Result<()> {
    eprintln!(
        "[build] all={} alt={} out={} min_pop={}",
        all_zip.display(),
        alt_zip.display(),
        out_db.display(),
        min_pop
    );

    // 1) Parse allCountries directly from ZIP
    let records = with_zip_member(all_zip, "allCountries.txt", |reader| {
        parse_allcountries_chunked_reader(reader, min_pop)
    })?;
    if records.is_empty() {
        bail!("no records parsed from allCountries (min_pop too high?)");
    }

    // 2) id presence set (only for kept records)
    let mut id_present: FastIdSet =
        HashSet::with_capacity_and_hasher(records.len() * 2, RandomState::new());
    for r in &records {
        id_present.insert(r.id);
    }

    // 3) key -> postings
    let mut key_to_ids: FastBuildMap =
        HashMap::with_capacity_and_hasher(records.len() * 2, RandomState::new());

    // 4) Seed from primary names (lowercased keys)
    {
        let prog = Progress::new("seed_names", 1_000_000);
        let mut n: u64 = 0;
        for r in &records {
            if let Some(k) = norm_key(&r.name) {
                key_to_ids.entry(k).or_default().push(r.id);
            }
            if let Some(k) = norm_key(&r.ascii_name) {
                key_to_ids.entry(k).or_default().push(r.id);
            }
            n += 1;
            prog.tick(n, &format!("keys={}", key_to_ids.len()));
        }
        prog.done(n, &format!("keys={}", key_to_ids.len()));
    }

    // 5) Merge alternate names directly from ZIP (lowercased keys)
    with_zip_member(alt_zip, "alternateNamesV2.txt", |reader| {
        merge_altnames_chunked_reader(reader, &id_present, &mut key_to_ids)
    })?;

    // 6) Sort + dedup postings
    {
        let prog = Progress::new("dedup", 2_000_000);
        let mut i: u64 = 0;
        for ids in key_to_ids.values_mut() {
            if ids.len() > 1 {
                ids.sort_unstable();
                ids.dedup();
            }
            i += 1;
            prog.tick(i, "");
        }
        prog.done(i, &format!("keys={}", key_to_ids.len()));
    }

    let total_postings: usize = key_to_ids.values().map(|v| v.len()).sum();
    eprintln!(
        "[index] keys={} total_postings={} records={}",
        key_to_ids.len(),
        total_postings,
        records.len()
    );

    // 7) Write DB
    write_db(out_db, &key_to_ids, &records)?;
    Ok(())
}

/* -------------------------
   parse allCountries (chunked + parallel per chunk)
-------------------------- */

fn parse_allcountries_chunked_reader<R: BufRead>(mut r: R, min_pop: u32) -> Result<Vec<GeoRecord>> {
    let prog = Progress::new("all_lines", 1_000_000);
    let mut out: Vec<GeoRecord> = Vec::new();
    let mut total_lines: u64 = 0;
    let mut kept: u64 = 0;

    loop {
        let mut chunk: Vec<String> = Vec::with_capacity(CHUNK_LINES);
        for _ in 0..CHUNK_LINES {
            let mut line = String::new();
            let n = r.read_line(&mut line)?;
            if n == 0 {
                break;
            }
            if line.ends_with('\n') {
                line.pop();
                if line.ends_with('\r') {
                    line.pop();
                }
            }
            chunk.push(line);
        }

        if chunk.is_empty() {
            break;
        }

        total_lines += chunk.len() as u64;
        prog.tick(total_lines, &format!("kept={}", kept));

        let recs: Vec<GeoRecord> = chunk
            .par_iter()
            .filter_map(|line| parse_allcountries_line(line, min_pop).ok())
            .collect();

        kept += recs.len() as u64;
        out.extend(recs);
    }

    prog.done(total_lines, &format!("kept={}", kept));
    Ok(out)
}

// Minimal columns used (tab-separated):
// 0 id, 1 name, 2 asciiname, 4 lat, 5 lon, 6 feat_class, 7 feat_code,
// 8 country, 10 admin1, 11 admin2, 14 population
fn parse_allcountries_line(line: &str, min_pop: u32) -> Result<GeoRecord> {
    let mut it = line.split('\t');

    let id_s = it.next().ok_or_else(|| anyhow!("missing id"))?;
    let name = it.next().ok_or_else(|| anyhow!("missing name"))?;
    let ascii_name = it.next().ok_or_else(|| anyhow!("missing asciiname"))?;

    // skip 3
    let _ = it.next().ok_or_else(|| anyhow!("missing col3"))?;

    let lat_s = it.next().ok_or_else(|| anyhow!("missing lat"))?;
    let lon_s = it.next().ok_or_else(|| anyhow!("missing lon"))?;
    let feat_class_s = it.next().ok_or_else(|| anyhow!("missing feat_class"))?;
    let feat_code = it.next().ok_or_else(|| anyhow!("missing feat_code"))?;
    let country = it.next().ok_or_else(|| anyhow!("missing country"))?;

    // skip 9
    let _ = it.next().ok_or_else(|| anyhow!("missing col9"))?;
    let admin1 = it.next().ok_or_else(|| anyhow!("missing admin1"))?;
    let admin2 = it.next().ok_or_else(|| anyhow!("missing admin2"))?;

    // skip 12, 13
    let _ = it.next().ok_or_else(|| anyhow!("missing col12"))?;
    let _ = it.next().ok_or_else(|| anyhow!("missing col13"))?;

    let population_s = it.next().ok_or_else(|| anyhow!("missing population"))?;

    let id: u32 = id_s.parse()?;
    let lat: f32 = lat_s.parse::<f32>()?;
    let lon: f32 = lon_s.parse::<f32>()?;
    let feat_class = feat_class_s.as_bytes().get(0).copied().unwrap_or(b'?');
    let population: u32 = population_s.parse().unwrap_or(0);

    if population < min_pop {
        bail!("below min_pop");
    }

    Ok(GeoRecord {
        id,
        name: name.to_string(),
        ascii_name: ascii_name.to_string(),
        country: country.to_string(),
        admin1: admin1.to_string(),
        admin2: admin2.to_string(),
        lat,
        lon,
        feat_class,
        feat_code: feat_code.to_string(),
        population,
    })
}

/* -------------------------
   parse alternateNamesV2 (chunked)
-------------------------- */

fn merge_altnames_chunked_reader<R: BufRead>(
    mut r: R,
    id_present: &FastIdSet,
    key_to_ids: &mut FastBuildMap,
) -> Result<()> {
    let prog = Progress::new("alt_lines", 1_000_000);
    let mut total_lines: u64 = 0;
    let mut kept_pairs: u64 = 0;

    loop {
        let mut chunk: Vec<String> = Vec::with_capacity(CHUNK_LINES);
        for _ in 0..CHUNK_LINES {
            let mut line = String::new();
            let n = r.read_line(&mut line)?;
            if n == 0 {
                break;
            }
            if line.ends_with('\n') {
                line.pop();
                if line.ends_with('\r') {
                    line.pop();
                }
            }
            chunk.push(line);
        }

        if chunk.is_empty() {
            break;
        }

        total_lines += chunk.len() as u64;
        prog.tick(
            total_lines,
            &format!("kept_pairs={} keys={}", kept_pairs, key_to_ids.len()),
        );

        let pairs: Vec<(String, u32)> = chunk
            .par_iter()
            .filter_map(|line| parse_alt_pair(line, id_present).ok().flatten())
            .collect();

        kept_pairs += pairs.len() as u64;
        for (k, id) in pairs {
            key_to_ids.entry(k).or_default().push(id);
        }
    }

    prog.done(
        total_lines,
        &format!("kept_pairs={} keys={}", kept_pairs, key_to_ids.len()),
    );
    Ok(())
}

fn parse_alt_pair(line: &str, id_present: &FastIdSet) -> Result<Option<(String, u32)>> {
    let mut it = line.split('\t');

    let _alt_id = match it.next() {
        Some(v) => v,
        None => return Ok(None),
    };
    let geoname_s = match it.next() {
        Some(v) => v,
        None => return Ok(None),
    };
    let _iso = match it.next() {
        Some(v) => v,
        None => return Ok(None),
    };
    let alt_name = match it.next() {
        Some(v) => v,
        None => return Ok(None),
    };

    let geoname_id: u32 = match geoname_s.parse() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    if !id_present.contains(&geoname_id) {
        return Ok(None);
    }

    match norm_key(alt_name) {
        Some(k) => Ok(Some((k, geoname_id))),
        None => Ok(None),
    }
}

/* -------------------------
   write db
-------------------------- */

fn write_db(out: &Path, key_to_ids: &FastBuildMap, records: &[GeoRecord]) -> Result<()> {
    // keys sorted for FST builder
    let mut keys: Vec<(&str, &SmallVec<[u32; 2]>)> =
        key_to_ids.iter().map(|(k, v)| (k.as_str(), v)).collect();
    keys.sort_unstable_by(|a, b| a.0.cmp(b.0));

    let mut postings_blob: Vec<u8> = Vec::new();
    let mut fst_bytes: Vec<u8> = Vec::new();

    eprintln!("[fst] building for {} keys", keys.len());
    let fst_start = Instant::now();
    {
        let mut b = MapBuilder::new(&mut fst_bytes)?;
        let prog = Progress::new("post+fst", 1_000_000);

        for (i, (k, ids)) in keys.iter().enumerate() {
            let off = postings_blob.len() as u64;

            let enc = encode_delta_varints(ids);
            write_var_u32(&mut postings_blob, enc.len() as u32);
            postings_blob.extend_from_slice(&enc);

            b.insert(k, off)?;
            prog.tick(
                i as u64,
                &format!("keys={} post_bytes={}", i + 1, postings_blob.len()),
            );
        }
        b.finish()?;
        prog.done(keys.len() as u64, &format!("post_bytes={}", postings_blob.len()));
    }
    eprintln!(
        "[fst] bytes={} build_t={:.2}s",
        fst_bytes.len(),
        fst_start.elapsed().as_secs_f64()
    );

    // records sorted by id + offsets table
    let mut recs = records.to_vec();
    recs.sort_by_key(|r| r.id);

    let mut ids: Vec<u32> = Vec::with_capacity(recs.len());
    let mut rec_offs: Vec<u64> = Vec::with_capacity(recs.len());
    let mut records_blob: Vec<u8> = Vec::new();

    let prog2 = Progress::new("records", 1_000_000);
    for (i, r) in recs.iter().enumerate() {
        let off = records_blob.len() as u64;
        ids.push(r.id);
        rec_offs.push(off);
        write_record(&mut records_blob, r)?;
        prog2.tick(i as u64, &format!("bytes={}", records_blob.len()));
    }
    prog2.done(recs.len() as u64, &format!("bytes={}", records_blob.len()));

    let mut offsets_blob: Vec<u8> = Vec::new();
    offsets_blob.write_u32::<LittleEndian>(ids.len() as u32)?;
    for id in &ids {
        offsets_blob.write_u32::<LittleEndian>(*id)?;
    }
    for off in &rec_offs {
        offsets_blob.write_u64::<LittleEndian>(*off)?;
    }

    // file layout: MAGIC + VERSION + lens + sections
    let mut w = BufWriter::new(File::create(out)?);
    w.write_all(MAGIC)?;
    w.write_u32::<LittleEndian>(VERSION)?;
    w.write_u64::<LittleEndian>(fst_bytes.len() as u64)?;
    w.write_u64::<LittleEndian>(postings_blob.len() as u64)?;
    w.write_u64::<LittleEndian>(records_blob.len() as u64)?;
    w.write_u64::<LittleEndian>(offsets_blob.len() as u64)?;
    w.write_all(&fst_bytes)?;
    w.write_all(&postings_blob)?;
    w.write_all(&records_blob)?;
    w.write_all(&offsets_blob)?;
    w.flush()?;
    Ok(())
}

fn write_record(buf: &mut Vec<u8>, r: &GeoRecord) -> Result<()> {
    buf.write_u32::<LittleEndian>(r.id)?;
    buf.write_f32::<LittleEndian>(r.lat)?;
    buf.write_f32::<LittleEndian>(r.lon)?;
    buf.write_u32::<LittleEndian>(r.population)?;
    buf.push(r.feat_class);

    // Stored values keep original casing (only index keys are lowercased)
    write_lp_str(buf, &r.name);
    write_lp_str(buf, &r.country);
    write_lp_str(buf, &r.admin1);
    write_lp_str(buf, &r.admin2);
    write_lp_str(buf, &r.feat_code);
    Ok(())
}

fn write_lp_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    write_var_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

/* -------------------------
   compact postings encoding
-------------------------- */

fn encode_delta_varints(ids: &[u32]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut prev = 0u32;
    for &id in ids {
        let d = id.wrapping_sub(prev);
        write_var_u32(&mut out, d);
        prev = id;
    }
    out
}

fn write_var_u32(buf: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        buf.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}