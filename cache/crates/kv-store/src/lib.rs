//! 공통 In-Memory Key-Value 저장소.
//!
//! [`KvStore`] Trait과 HashMap 기반 구현체 [`MemoryStore`]를 제공한다.
//!
//! # 오류 처리 규칙
//!
//! - 존재하지 않는 Key는 오류가 아니다. `get`은 `None`(MISS), `delete`는 `false`를 반환한다.
//! - Trait 메서드는 `Result`를 반환하지 않는다. 메모리 저장소의 연산은 실패하지 않으며,
//!   메모리 할당 실패는 Rust 표준 동작에 따라 프로세스를 중단한다.
//! - 빈 Key와 빈 Value도 유효한 데이터다. 빈 Value가 저장된 Key의 `get`은
//!   `Some(vec![])`을 반환하므로 MISS(`None`)와 구분된다.
//!
//! # 예제
//!
//! ```
//! use kv_store::{KvStore, MemoryStore};
//!
//! let mut store = MemoryStore::new();
//! store.set(b"user:1".to_vec(), b"alice".to_vec());
//!
//! assert_eq!(store.get(b"user:1"), Some(b"alice".to_vec()));
//! assert_eq!(store.get(b"user:2"), None); // MISS
//!
//! assert!(store.delete(b"user:1"));
//! assert!(!store.exists(b"user:1"));
//! assert_eq!(store.count(), 0);
//! ```

mod memory;

pub use memory::MemoryStore;

/// Key-Value 저장소의 공통 연산.
///
/// Key와 Value는 임의의 바이트열이다. 조회 계열 메서드는 Key를 슬라이스로 빌려 받고,
/// `set`은 Key와 Value의 소유권을 받아 추가 복사 없이 저장한다.
pub trait KvStore {
    /// `key`에 저장된 값의 복사본을 반환한다. Key가 없으면 `None`(MISS)을 반환한다.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// `key`에 `value`를 저장한다. 같은 Key가 이미 있으면 기존 값을 덮어쓴다.
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);

    /// `key`를 삭제한다. 삭제했으면 `true`, Key가 없었으면 `false`를 반환한다.
    fn delete(&mut self, key: &[u8]) -> bool;

    /// `key`가 존재하면 `true`를 반환한다.
    fn exists(&self, key: &[u8]) -> bool;

    /// 현재 저장된 엔트리 개수를 반환한다.
    fn count(&self) -> usize;
}
