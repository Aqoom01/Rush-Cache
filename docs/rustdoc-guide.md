# Rust 문서 주석 및 cargo doc 가이드

공통 모듈의 API 문서는 소스 코드의 문서 주석으로 작성하고, `cargo doc`으로 HTML 문서를 생성해 확인한다.

## 1. 주석 종류

| 표기 | 설명 대상 | 문서 생성 |
|---|---|---|
| `//` | 코드를 읽는 사람 (일반 주석) | X |
| `///` | 바로 아래 항목 (함수, Trait, 구조체 등) | O |
| `//!` | 주석을 담고 있는 항목 (`lib.rs` 맨 위에 쓰면 크레이트 전체) | O |

```rust
//! 공통 In-Memory Key-Value 저장소.        ← 크레이트 소개

/// Key-Value 저장소의 공통 연산.           ← 아래 Trait 설명
pub trait KvStore {
    /// `key`에 저장된 값의 복사본을 반환한다. ← 아래 메서드 설명
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
}
```

## 2. 작성 기준

- 문서 주석(`///`, `//!`)에는 **무엇을 하고 어떻게 쓰는지**를 쓴다. 반환값의 의미, 실패·MISS 조건, 사용 예제 등 다른 연구자가 코드를 열지 않고도 알아야 할 내용이다.
- 일반 주석(`//`)에는 코드만으로 알 수 없는 **이유**만 쓴다. 코드를 그대로 반복하는 주석은 쓰지 않는다.
- 첫 문장은 한 줄 요약으로 쓴다. 문서 목록에는 첫 문단만 표시된다.
- 공개(`pub`) 항목에는 문서 주석을 단다.

문서 주석은 Markdown으로 작성하며, 다음 표기를 자주 쓴다.

| 표기 | 의미 |
|---|---|
| `# 예제`, `# 오류 처리 규칙` | 섹션 제목 |
| `` [`KvStore`] `` | 해당 타입 문서로 연결되는 링크 |
| `` ``` `` 코드 블록 | 사용 예제. **`cargo test` 시 실제로 컴파일·실행된다** (Doc-test) |

예제 코드는 테스트로 실행되므로 API가 바뀌었는데 예제를 고치지 않으면 테스트가 실패한다.

## 3. 문서 생성 및 확인

`cache/`에서 실행한다.

```bash
cargo doc --workspace --no-deps        # 전체 크레이트 문서 생성
cargo doc -p kv-store --no-deps        # 특정 크레이트만 생성
```

생성 위치: `cache/target/doc/<크레이트 이름>/index.html` (크레이트 이름의 `-`는 `_`로 바뀐다. 예: `kv_store`)

WSL에서는 `cargo doc --open`으로 브라우저가 열리지 않을 수 있으므로 다음 방법으로 연다.

```bash
explorer.exe "$(wslpath -w target/doc/kv_store/index.html)"
```

또는 Windows 탐색기·브라우저 주소창에 아래 형식의 경로를 입력한다.

```
\\wsl.localhost\Ubuntu\<저장소 경로>\cache\target\doc\kv_store\index.html
```

- `target/`은 Git에 올라가지 않으므로 각자 생성해서 확인한다.
- 코드나 주석을 수정한 뒤에는 `cargo doc`을 다시 실행해야 반영된다.
