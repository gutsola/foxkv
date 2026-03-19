use std::collections::{BTreeMap, BTreeSet};

use crate::command::shared::zset::ZSet;

const MAGIC: &[u8; 4] = b"FKV1";
const TYPE_HASH: u8 = 1;
const TYPE_LIST: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_ZSET: u8 = 4;

#[derive(Clone)]
pub enum TypedValue {
    #[allow(dead_code)]
    String(Vec<u8>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    List(Vec<Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
    ZSet(ZSet),
}

pub fn decode_value(raw: &[u8]) -> Result<TypedValue, String> {
    if raw.len() < 5 || &raw[..4] != MAGIC {
        return Ok(TypedValue::String(raw.to_vec()));
    }
    let kind = raw[4];
    let mut cursor = 5_usize;
    match kind {
        TYPE_HASH => decode_hash(raw, &mut cursor).map(TypedValue::Hash),
        TYPE_LIST => decode_list(raw, &mut cursor).map(TypedValue::List),
        TYPE_SET => decode_set(raw, &mut cursor).map(TypedValue::Set),
        TYPE_ZSET => decode_zset(raw, &mut cursor).map(TypedValue::ZSet),
        _ => Err("ERR value encoding is corrupted".to_string()),
    }
}

pub fn encode_hash(map: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(MAGIC);
    out.push(TYPE_HASH);
    append_u32(&mut out, map.len() as u32);
    for (field, value) in map {
        append_blob(&mut out, field);
        append_blob(&mut out, value);
    }
    out
}

pub fn encode_list(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(MAGIC);
    out.push(TYPE_LIST);
    append_u32(&mut out, items.len() as u32);
    for item in items {
        append_blob(&mut out, item);
    }
    out
}

pub fn encode_set(set: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(MAGIC);
    out.push(TYPE_SET);
    append_u32(&mut out, set.len() as u32);
    for member in set {
        append_blob(&mut out, member);
    }
    out
}

pub fn encode_zset(zset: &ZSet) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(MAGIC);
    out.push(TYPE_ZSET);
    append_u32(&mut out, zset.len() as u32);
    for (member, score) in zset.iter() {
        append_blob(&mut out, &member);
        out.extend_from_slice(&score.to_le_bytes());
    }
    out
}

pub fn wrong_type_error() -> String {
    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
}

fn decode_hash(raw: &[u8], cursor: &mut usize) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> {
    let len = read_u32(raw, cursor)? as usize;
    let mut map = BTreeMap::new();
    for _ in 0..len {
        let field = read_blob(raw, cursor)?;
        let value = read_blob(raw, cursor)?;
        map.insert(field, value);
    }
    Ok(map)
}

fn decode_list(raw: &[u8], cursor: &mut usize) -> Result<Vec<Vec<u8>>, String> {
    let len = read_u32(raw, cursor)? as usize;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(read_blob(raw, cursor)?);
    }
    Ok(out)
}

fn decode_set(raw: &[u8], cursor: &mut usize) -> Result<BTreeSet<Vec<u8>>, String> {
    let len = read_u32(raw, cursor)? as usize;
    let mut set = BTreeSet::new();
    for _ in 0..len {
        set.insert(read_blob(raw, cursor)?);
    }
    Ok(set)
}

fn decode_zset(raw: &[u8], cursor: &mut usize) -> Result<ZSet, String> {
    let len = read_u32(raw, cursor)? as usize;
    let mut zset = ZSet::new();
    for _ in 0..len {
        let member = read_blob(raw, cursor)?;
        if *cursor + 8 > raw.len() {
            return Err("ERR value encoding is corrupted".to_string());
        }
        let mut score_bytes = [0_u8; 8];
        score_bytes.copy_from_slice(&raw[*cursor..*cursor + 8]);
        *cursor += 8;
        let score = f64::from_le_bytes(score_bytes);
        if !score.is_finite() {
            return Err("ERR value encoding is corrupted".to_string());
        }
        zset.add(member, score)
            .map_err(|e| format!("decode zset: {e}"))?;
    }
    Ok(zset)
}

fn append_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(raw: &[u8], cursor: &mut usize) -> Result<u32, String> {
    if *cursor + 4 > raw.len() {
        return Err("ERR value encoding is corrupted".to_string());
    }
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&raw[*cursor..*cursor + 4]);
    *cursor += 4;
    Ok(u32::from_le_bytes(bytes))
}

fn append_blob(out: &mut Vec<u8>, blob: &[u8]) {
    append_u32(out, blob.len() as u32);
    out.extend_from_slice(blob);
}

fn read_blob(raw: &[u8], cursor: &mut usize) -> Result<Vec<u8>, String> {
    let len = read_u32(raw, cursor)? as usize;
    if *cursor + len > raw.len() {
        return Err("ERR value encoding is corrupted".to_string());
    }
    let value = raw[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(value)
}

