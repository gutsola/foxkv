use crate::storage::model::ValueEntry;

pub trait StorageEngine: Send + Sync {
    fn get_entry(&self, key: &[u8]) -> Option<ValueEntry>;
    fn put_entry(&self, key: &[u8], entry: ValueEntry);
    fn put_if_absent(&self, key: &[u8], entry: ValueEntry) -> bool;
    fn put_if_present(&self, key: &[u8], entry: ValueEntry) -> bool;
    fn remove_entry(&self, key: &[u8]) -> bool;
    fn contains_live_key(&self, key: &[u8]) -> bool;
    fn iter_live_keys(&self) -> Vec<Vec<u8>>;
    fn scan_live_keys(&self, cursor: usize, count: usize) -> (usize, Vec<Vec<u8>>);
}
