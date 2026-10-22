/**
 * 노드 로컬 Key-Value 저장소.
 *
 * <p>책임: 한 노드 안에서 Key-Value를 저장·조회·삭제한다. 복제·라우팅은 다루지 않는다.
 *
 * <p>상태: {@link io.openkache.core.kv.KvEngine}이 단일 노드 인메모리 GET/SET/DELETE/EXISTS/COUNT를 구현한다. Key마다 고정
 * worker로 샤딩하고, 저장소는 그 owner worker thread에서만 접근한다는 {@code runtime} 규칙을 따른다. COUNT는 Key가 없어 모든
 * worker에 보내 합산한다.
 *
 * <p>SCAN/RANGE(선택, B3 Live Migration 요구)는 {@code Command}가 아니라 {@link io.openkache.core.kv.KvEngine#scan}·
 * {@link io.openkache.core.kv.KvEngine#range}로 제공한다. {@code Result}가 값 하나만 담기 때문이며, 결과는
 * {@link io.openkache.core.kv.ScanResult}(상태 + {@link io.openkache.core.kv.KvEntry} 목록, Key 바이트 순서 정렬)다. SCAN은
 * Key 조건을 {@code Predicate<Bytes>}로 받으므로 Tablet 해시 범위처럼 KV가 모르는 기준도 호출자가 넣을 수 있다. 모든 worker를
 * 돌아 합치므로 노드 전체의 한 순간 snapshot은 아니다. TCP opcode는 없다.
 *
 * <h2>오류 처리 규칙</h2>
 *
 * <p>예상 가능한 실패는 예외가 아니라 {@link io.openkache.core.api.Result#status()}로 표현한다. 이 패키지가 내는 상태는 아래가
 * 전부다. 예외는 호출자의 프로그래밍 오류({@code null} 인자)일 때만 던진다.
 *
 * <ul>
 *   <li>{@link io.openkache.core.api.ResultStatus#OK}: 연산 성공. GET은 값 복사본과 {@code valueVersion}, SET은 새
 *       {@code valueVersion}(1부터 SET마다 1 증가), DELETE는 삭제됨, EXISTS는 존재함(값 없이 {@code valueVersion}만), COUNT는
 *       개수(8-byte big-endian {@code value})를 뜻한다.
 *   <li>{@link io.openkache.core.api.ResultStatus#MISS}: 정상 응답이다. GET·EXISTS는 Key가 없음, DELETE는 지울 Key가 없었음을
 *       뜻한다. SET·COUNT는 MISS를 내지 않는다(COUNT는 비어 있으면 개수 0인 OK).
 *   <li>{@link io.openkache.core.api.ResultStatus#BUSY}: owner worker의 mailbox가 가득 차 요청을 받지 못했다. 호출자는
 *       기다리지 않고 즉시 이 결과를 받는다. 재시도 여부는 호출자가 정한다. COUNT·SCAN·RANGE는 어느 한 worker라도 가득 차면
 *       BUSY다.
 *   <li>{@link io.openkache.core.api.ResultStatus#TIMEOUT}: {@code RequestContext.deadline()}이 접수 시점 또는 owner worker가
 *       실행하려는 시점에 이미 지났다. 저장소는 바뀌지 않는다.
 *   <li>{@link io.openkache.core.api.ResultStatus#UNAVAILABLE}: 엔진이 {@code close()}됐다. 닫힌 뒤 들어온 요청과, 닫힐 때
 *       아직 완료되지 않은 요청 모두 이 상태로 끝난다.
 * </ul>
 *
 * <p>{@code BAD_REQUEST}(형식이 잘못된 요청)는 wire를 해석하는 server가 내며 이 패키지까지 오지 않는다. {@code MOVED},
 * {@code NOT_PRIMARY}, {@code STALE_EPOCH}, {@code CAPACITY_EXCEEDED}는 라우팅·복제·용량 한도가 없는 지금은 내지 않는다.
 * routingVersion과 ownershipEpoch는 항상 0이다.
 *
 * <p>아직 구현하지 않은 것: TTL 만료, 저장 용량 한도(eviction), 영속화. EXISTS/COUNT/SCAN/RANGE의 wire opcode는
 * {@code docs/protocol-v1.md}에 아직 없어 TCP로는 호출할 수 없다. 복제·Tablet 연동 시 필요한 epoch 검사는
 * {@link io.openkache.core.spi.EpochStore}와 함께 그 연구 패키지가 책임진다.
 */
package io.openkache.core.kv;
