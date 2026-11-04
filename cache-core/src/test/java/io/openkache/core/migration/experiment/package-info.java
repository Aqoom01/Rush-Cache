/**
 * 설계 §6.1의 SC1~SC6 baseline 실험. 서버·네트워크 대신 단일 thread의 논리 tick으로 실행한다.
 *
 * <p>최소 저장소와 전송 adapter, 외부 조건부 commit 실행기, seed 기반 요청열과 main runner를 제공한다.
 * JUnit에서 실험을 자동 실행하지 않는다. 결과는 실제 실험 뒤
 * experiments/b3-live-migration/migration-comparison/에 생성한다.
 */
package io.openkache.core.migration.experiment;
