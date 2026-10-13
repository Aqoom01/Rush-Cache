# OpenStd Git 컨벤션

브랜치·커밋 규칙과 기본 작업 흐름을 정리한다. MR 작성·리뷰·병합 규칙은 [MR 컨벤션](mr-convention.md)을 따른다.

## 1. 브랜치 전략

| 브랜치 | 용도 |
|---|---|
| `master` | 검증된 결과물 및 마일스톤 관리 |
| `develop` | 개발 코드 통합 |
| `feat/*` | 새로운 기능 구현 |
| `fix/*` | 버그 수정 |
| `refactor/*` | 리팩터링 |
| `test/*` | 테스트 |
| `docs/*` | 문서 |
| `chore/*` | 환경 설정 및 유지보수 |
| `experiment/*` | 연구 실험 및 벤치마크 |

- 기본 흐름: `develop` → 작업 브랜치 → MR → `develop`
- `master`는 마일스톤을 완료할 때 `develop`에서 MR로 병합한다.
- `master`와 `develop`에는 직접 push하지 않는다.
- 작업 브랜치는 Jira Task 단위로, 최신 `develop`에서 만든다.
- `release/*`, `hotfix/*`는 현재 사용하지 않는다.

**브랜치 이름:** `<type>/<영문 요약>`

```
feat/kv-store
docs/git-convention
```

- 영문 요약은 소문자와 하이픈(`-`)으로 쓴다.
- Jira 이슈 키는 브랜치 이름에 넣지 않고 MR 제목에 넣는다([MR 컨벤션](mr-convention.md)).
- 기존 `env/*` 브랜치는 그대로 두고, 새로 만드는 브랜치부터 이 규칙을 적용한다.

## 2. 커밋 컨벤션

Conventional Commits 형식을 쓴다.

```
<type>: <한국어 설명>
```

| type | 용도 | 예시 |
|---|---|---|
| `feat` | 새로운 기능 | `feat: KV Store GET/SET 구현` |
| `fix` | 버그 수정 | `fix: 장애 감지 타임아웃 오류 수정` |
| `refactor` | 동작 변화 없는 구조 개선 | `refactor: 메시지 처리 로직 분리` |
| `test` | 테스트 추가·수정 | `test: KV Store 단위 테스트 추가` |
| `docs` | 문서 | `docs: Git 협업 컨벤션 작성` |
| `chore` | 환경 설정, 의존성, CI | `chore: Cargo Workspace 설정` |
| `experiment` | 연구 실험, 벤치마크 | `experiment: 복제 지연 시간 측정 추가` |

- 하나의 커밋에는 하나의 논리적인 변경을 담는다.
- 제목은 무엇을 바꿨는지 알 수 있게 간결하고 명확하게 쓴다.
- 개별 커밋에 Jira 이슈 키를 넣지 않아도 된다.

## 3. 기본 작업 흐름

1. **Jira Task 생성:** Jira에서 Task를 만들고 이슈 키를 확인한다(예: `S15P31D111-12`).
2. **develop 최신화**
3. **작업 브랜치 생성**
4. **개발 및 Commit:** MR 전에 `cache/`에서 fmt, clippy, test를 확인한다.
5. **Push**
6. **MR 생성 및 리뷰:** [MR 컨벤션](mr-convention.md)에 따라 MR을 만들고 리뷰를 받는다.
7. **develop Merge:** 승인 후 GitLab에서 Merge하고 로컬 브랜치를 정리한다.
8. **Jira 완료 확인:** 병합 후 Jira 이슈 상태를 확인한다.

```bash
# 1. Jira Task 생성 (Jira에서 진행) → 이슈 키 확인: S15P31D111-12

# 2. develop 최신화
git switch develop
git pull origin develop

# 3. 작업 브랜치 생성
git switch -c feat/kv-store

# 4. 개발 및 Commit
cd cache
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cd ..
git add .
git commit -m "feat: KV Store GET/SET 구현"

# 5. Push
git push -u origin feat/kv-store

# 6. MR 생성 및 리뷰 (GitLab에서 진행)
#    제목: [Feat] KV Store GET/SET 구현 (S15P31D111-12)

# 7. GitLab에서 Merge 후 로컬 정리 (Squash 병합된 브랜치는 -D로 삭제)
git switch develop
git pull origin develop
git branch -D feat/kv-store

# 8. Jira 완료 확인 (Jira에서 진행)
```
