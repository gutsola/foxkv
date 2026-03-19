use crate::resp::{append_bulk_response, append_integer_response};

pub fn append_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn append_bulk_items(out: &mut Vec<u8>, items: &[Vec<u8>]) {
    append_array_header(out, items.len());
    for item in items {
        append_bulk_response(out, Some(item));
    }
}

#[allow(dead_code)]
pub fn append_optional_bulk_items(out: &mut Vec<u8>, items: &[Option<Vec<u8>>]) {
    append_array_header(out, items.len());
    for item in items {
        append_bulk_response(out, item.as_deref());
    }
}

pub fn append_scan_response(out: &mut Vec<u8>, next_cursor: usize, items: &[Vec<u8>]) {
    append_array_header(out, 2);
    let cursor = next_cursor.to_string().into_bytes();
    append_bulk_response(out, Some(&cursor));
    append_bulk_items(out, items);
}

pub fn append_bool_integer(out: &mut Vec<u8>, value: bool) {
    append_integer_response(out, if value { 1 } else { 0 });
}

