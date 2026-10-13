# OpenStd MR 컨벤션

Merge Request 작성·리뷰·병합 규칙을 정리한다. 브랜치·커밋 규칙은 [Git 컨벤션](git-convention.md)을 따른다.

## 1. MR 제목

```
[Type] 작업 내용 (Jira 이슈 키)
```

```
[Feat] KV Store GET/SET 구현 (S15P31D111-12)
[Fix] 장애 감지 타임아웃 오류 수정 (S15P31D111-20)
[Chore] GitLab CI 설정 (S15P31D111-8)
```

- Type은 커밋 type과 같은 종류를 첫 글자만 대문자로 쓴다: `Feat`, `Fix`, `Refactor`, `Test`, `Docs`, `Chore`, `Experiment`
- Jira 이슈 키는 제목 맨 끝 괄호 안에 쓴다.
- Squash 병합 시 MR 제목이 그대로 `develop`의 커밋 메시지가 되어 이슈 키도 이력에 남는다.
- 작업 중인 MR은 GitLab의 Draft로 만들어도 된다.

## 2. MR 본문과 이슈 연결

- 본문은 `.gitlab/merge_request_templates/Default.md` 템플릿으로 작성한다.
- 변경 내용과 테스트 결과를 반드시 적는다.
- 관련 Jira 이슈를 본문 마지막에 연결한다.

```
Closes S15P31D111-12
```

하나의 Task를 여러 MR로 나누면 중간 MR에는 `Refs`, 마지막 MR에는 `Closes`를 쓴다.

```
Refs S15P31D111-12     ← 중간 MR
Closes S15P31D111-12   ← 마지막 MR
```

## 3. 리뷰와 병합

- 모든 작업은 MR을 통해 `develop`에 병합한다.
- MR 작성자를 Assignee로 지정한다.
- Reviewer는 최소 1명 지정한다.
- 공통 인터페이스(`common` 등)를 변경하면 영향받는 담당자에게 리뷰를 요청한다.
- 리뷰 의견을 반영하고 미해결 스레드가 없는지 확인한 뒤 Merge한다.
- 리뷰 승인 후 작성자가 직접 Merge할 수 있다.
- 작업 브랜치를 병합할 때는 **Squash**와 **Source Branch 삭제**를 권장한다.
- `develop` → `master` MR은 Squash하지 않는다. Squash하면 두 브랜치의 이력이 갈라져 이후 병합마다 충돌이 생긴다.

## 4. 라벨과 Jira 연동

**라벨:** 현재 GitLab에서 쓰는 라벨(`feature`, `fix`, `chore`, `test`, `benchmark` 등)을 그대로 쓴다.

**Jira 연동:** MR 병합 후 Jira 이슈 상태를 직접 확인한다. MR 제목·본문의 이슈 키로 Jira에 연결되거나 상태가 자동 전환되는지는 실제 GitLab–Jira 연동 설정에 따른다.
