/**
 * 부하 기반 배치. B2(Load-aware Balancing + Split/Merge)가 사용한다.
 *
 * <p>책임: QPS·CPU·IOPS 부하 입력을 받아 Tablet 재배치 계획을 세우고, Hot Tablet Split과 Cold Tablet Merge를 결정한다.
 *
 * <p>상태: 미구현. 계획의 실행(데이터 이동)은 {@code migration}이 담당한다. Tablet 구조는 {@code tablet} 패키지 합의에
 * 따른다.
 */
package io.openkache.core.balance;
