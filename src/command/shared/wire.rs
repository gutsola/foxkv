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

#[cfg(test)]
mod tests {
    use super::{
        append_array_header, append_bool_integer, append_bulk_items, append_optional_bulk_items,
        append_scan_response,
    };

    #[test]
    fn append_array_header_writes_resp_array_prefix() {
        let mut out = Vec::new();
        append_array_header(&mut out, 3);
        assert_eq!(out, b"*3\r\n");
    }

    #[test]
    fn append_bulk_items_writes_array_of_bulk_strings() {
        let mut out = Vec::new();
        let items = vec![b"foo".to_vec(), b"bar".to_vec()];
        append_bulk_items(&mut out, &items);
        assert_eq!(out, b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn append_optional_bulk_items_writes_nil_for_none_values() {
        let mut out = Vec::new();
        let items = vec![Some(b"a".to_vec()), None];
        append_optional_bulk_items(&mut out, &items);
        assert_eq!(out, b"*2\r\n$1\r\na\r\n$-1\r\n");
    }

    #[test]
    fn append_scan_response_writes_cursor_and_nested_items() {
        let mut out = Vec::new();
        let items = vec![b"k1".to_vec(), b"k2".to_vec()];
        append_scan_response(&mut out, 5, &items);
        assert_eq!(out, b"*2\r\n$1\r\n5\r\n*2\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
    }

    #[test]
    fn append_bool_integer_writes_one_or_zero() {
        let mut out = Vec::new();
        append_bool_integer(&mut out, true);
        append_bool_integer(&mut out, false);
        assert_eq!(out, b":1\r\n:0\r\n");
    }
}
