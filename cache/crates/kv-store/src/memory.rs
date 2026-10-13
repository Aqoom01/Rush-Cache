use std::collections::HashMap;

use crate::KvStore;

/// `HashMap` 기반 In-Memory [`KvStore`] 구현체.
///
/// 내부 동기화는 없다. 여러 스레드에서 공유하려면 호출자가 `Mutex` 등으로 감싼다.
#[derive(Debug, Default)]
pub struct MemoryStore {
    entries: HashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryStore {
    /// 빈 저장소를 만든다.
    pub fn new() -> Self {
        Self::default()
    }
}

impl KvStore for MemoryStore {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.entries.get(key).cloned()
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.entries.insert(key, value);
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        self.entries.remove(key).is_some()
    }

    fn exists(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
    }

    fn count(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_value_after_set() {
        let mut store = MemoryStore::new();
        store.set(b"key".to_vec(), b"value".to_vec());

        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn set_overwrites_existing_key() {
        let mut store = MemoryStore::new();
        store.set(b"key".to_vec(), b"old".to_vec());
        store.set(b"key".to_vec(), b"new".to_vec());

        assert_eq!(store.get(b"key"), Some(b"new".to_vec()));
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn get_missing_key_returns_none() {
        let store = MemoryStore::new();

        assert_eq!(store.get(b"missing"), None);
    }

    #[test]
    fn get_returns_independent_copy() {
        let mut store = MemoryStore::new();
        store.set(b"key".to_vec(), b"value".to_vec());

        let mut copy = store.get(b"key").unwrap();
        copy.clear();

        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn delete_existing_key_returns_true() {
        let mut store = MemoryStore::new();
        store.set(b"key".to_vec(), b"value".to_vec());

        assert!(store.delete(b"key"));
    }

    #[test]
    fn delete_missing_key_returns_false() {
        let mut store = MemoryStore::new();

        assert!(!store.delete(b"missing"));
    }

    #[test]
    fn get_after_delete_returns_none() {
        let mut store = MemoryStore::new();
        store.set(b"key".to_vec(), b"value".to_vec());
        store.delete(b"key");

        assert_eq!(store.get(b"key"), None);
        assert!(!store.delete(b"key"));
    }

    #[test]
    fn exists_and_count_track_entries() {
        let mut store = MemoryStore::new();
        assert_eq!(store.count(), 0);
        assert!(!store.exists(b"a"));

        store.set(b"a".to_vec(), b"1".to_vec());
        store.set(b"b".to_vec(), b"2".to_vec());
        assert!(store.exists(b"a"));
        assert!(store.exists(b"b"));
        assert_eq!(store.count(), 2);

        store.delete(b"a");
        assert!(!store.exists(b"a"));
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn empty_key_and_value_are_valid() {
        let mut store = MemoryStore::new();
        store.set(Vec::new(), b"value".to_vec());
        store.set(b"key".to_vec(), Vec::new());

        assert_eq!(store.get(b""), Some(b"value".to_vec()));
        // 빈 Value는 MISS(None)와 구분된다.
        assert_eq!(store.get(b"key"), Some(Vec::new()));
        assert!(store.exists(b""));
        assert_eq!(store.count(), 2);
    }

    #[test]
    fn binary_data_round_trips() {
        let mut store = MemoryStore::new();
        let key = vec![0x00, 0xFF, 0xC3, 0x28]; // NUL과 잘못된 UTF-8 시퀀스 포함
        let value: Vec<u8> = (0..=255).collect();
        store.set(key.clone(), value.clone());

        assert_eq!(store.get(&key), Some(value));
        assert_eq!(store.get(&[0x00, 0xFF]), None);
    }

    #[test]
    fn works_through_trait_object() {
        let mut store: Box<dyn KvStore> = Box::new(MemoryStore::new());
        store.set(b"key".to_vec(), b"value".to_vec());

        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
        assert_eq!(store.count(), 1);
    }
}
