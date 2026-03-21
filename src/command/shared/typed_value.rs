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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    #[test]
    fn decode_value_returns_string_for_non_magic_payload() {
        let raw = b"plain-value".to_vec();
        let decoded = decode_value(&raw).expect("decode should succeed");
        match decoded {
            TypedValue::String(v) => assert_eq!(v, raw),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn hash_roundtrip_preserves_fields_and_values() {
        let mut map = BTreeMap::new();
        map.insert(b"f1".to_vec(), b"v1".to_vec());
        map.insert(b"f2".to_vec(), b"v2".to_vec());

        let raw = encode_hash(&map);
        let decoded = decode_value(&raw).expect("decode should succeed");
        match decoded {
            TypedValue::Hash(got) => assert_eq!(got, map),
            _ => panic!("expected hash"),
        }
    }

    #[test]
    fn list_roundtrip_preserves_order() {
        let items = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let raw = encode_list(&items);
        let decoded = decode_value(&raw).expect("decode should succeed");
        match decoded {
            TypedValue::List(got) => assert_eq!(got, items),
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn set_roundtrip_preserves_all_members() {
        let mut set = BTreeSet::new();
        set.insert(b"a".to_vec());
        set.insert(b"b".to_vec());

        let raw = encode_set(&set);
        let decoded = decode_value(&raw).expect("decode should succeed");
        match decoded {
            TypedValue::Set(got) => assert_eq!(got, set),
            _ => panic!("expected set"),
        }
    }

    #[test]
    fn zset_roundtrip_preserves_members_and_scores() {
        let mut z = ZSet::new();
        z.add(b"a".to_vec(), 1.5).expect("valid score");
        z.add(b"b".to_vec(), 2.5).expect("valid score");

        let raw = encode_zset(&z);
        let decoded = decode_value(&raw).expect("decode should succeed");
        match decoded {
            TypedValue::ZSet(got) => {
                let got_items: Vec<_> = got.iter().collect();
                assert_eq!(got_items.len(), 2);
                assert_eq!(got_items[0], (b"a".to_vec(), 1.5));
                assert_eq!(got_items[1], (b"b".to_vec(), 2.5));
            }
            _ => panic!("expected zset"),
        }
    }

    #[test]
    fn decode_value_returns_error_for_unknown_type_tag() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"FKV1");
        raw.push(99);
        match decode_value(&raw) {
            Ok(_) => panic!("expected decode error"),
            Err(err) => assert_eq!(err, "ERR value encoding is corrupted"),
        }
    }

    #[test]
    fn decode_value_returns_error_for_truncated_hash_payload() {
        let mut map = BTreeMap::new();
        map.insert(b"field".to_vec(), b"value".to_vec());
        let mut raw = encode_hash(&map);
        raw.pop();
        match decode_value(&raw) {
            Ok(_) => panic!("expected decode error"),
            Err(err) => assert_eq!(err, "ERR value encoding is corrupted"),
        }
    }

    #[test]
    fn decode_value_returns_error_for_malformed_payloads_table_driven() {
        let mut malformed_cases: Vec<(&str, Vec<u8>)> = Vec::new();

        // hash: declares one entry but omits value blob.
        let mut hash_missing_value = Vec::new();
        hash_missing_value.extend_from_slice(b"FKV1");
        hash_missing_value.push(TYPE_HASH);
        hash_missing_value.extend_from_slice(&1_u32.to_le_bytes()); // len
        hash_missing_value.extend_from_slice(&1_u32.to_le_bytes()); // field len
        hash_missing_value.extend_from_slice(b"f"); // field bytes
        malformed_cases.push(("hash_missing_value_blob", hash_missing_value));

        // list: declares item blob len larger than remaining bytes.
        let mut list_blob_truncated = Vec::new();
        list_blob_truncated.extend_from_slice(b"FKV1");
        list_blob_truncated.push(TYPE_LIST);
        list_blob_truncated.extend_from_slice(&1_u32.to_le_bytes()); // len
        list_blob_truncated.extend_from_slice(&4_u32.to_le_bytes()); // item len
        list_blob_truncated.extend_from_slice(b"ab"); // only 2 bytes
        malformed_cases.push(("list_blob_truncated", list_blob_truncated));

        // set: declares two members but provides only one.
        let mut set_count_mismatch = Vec::new();
        set_count_mismatch.extend_from_slice(b"FKV1");
        set_count_mismatch.push(TYPE_SET);
        set_count_mismatch.extend_from_slice(&2_u32.to_le_bytes()); // len
        set_count_mismatch.extend_from_slice(&1_u32.to_le_bytes()); // member1 len
        set_count_mismatch.extend_from_slice(b"a"); // member1
        malformed_cases.push(("set_count_mismatch", set_count_mismatch));

        // zset: member exists but score bytes are incomplete.
        let mut zset_truncated_score = Vec::new();
        zset_truncated_score.extend_from_slice(b"FKV1");
        zset_truncated_score.push(TYPE_ZSET);
        zset_truncated_score.extend_from_slice(&1_u32.to_le_bytes()); // len
        zset_truncated_score.extend_from_slice(&1_u32.to_le_bytes()); // member len
        zset_truncated_score.extend_from_slice(b"m"); // member
        zset_truncated_score.extend_from_slice(&[0_u8; 4]); // needs 8 bytes
        malformed_cases.push(("zset_truncated_score", zset_truncated_score));

        // zset: member length says 2 bytes but only one byte provided.
        let mut zset_member_truncated = Vec::new();
        zset_member_truncated.extend_from_slice(b"FKV1");
        zset_member_truncated.push(TYPE_ZSET);
        zset_member_truncated.extend_from_slice(&1_u32.to_le_bytes()); // len
        zset_member_truncated.extend_from_slice(&2_u32.to_le_bytes()); // member len
        zset_member_truncated.extend_from_slice(b"x"); // only 1 byte
        malformed_cases.push(("zset_member_truncated", zset_member_truncated));

        for (name, raw) in malformed_cases {
            match decode_value(&raw) {
                Ok(_) => panic!("case {name}: expected decode error"),
                Err(err) => assert_eq!(err, "ERR value encoding is corrupted", "case {name}"),
            }
        }
    }

    #[test]
    fn decode_value_returns_error_for_non_finite_zset_score() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"FKV1");
        raw.push(4);
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.extend_from_slice(b"a");
        raw.extend_from_slice(&f64::NAN.to_le_bytes());

        match decode_value(&raw) {
            Ok(_) => panic!("expected decode error"),
            Err(err) => assert_eq!(err, "ERR value encoding is corrupted"),
        }
    }

    #[test]
    fn wrong_type_error_returns_expected_message() {
        assert_eq!(
            wrong_type_error(),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }
}

