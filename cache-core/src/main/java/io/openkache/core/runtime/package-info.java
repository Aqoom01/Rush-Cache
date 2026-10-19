/**
 * core의 실행 원칙.
 *
 * <ul>
 *   <li><b>단일 owner</b>: 변경 가능한 상태는 정확히 하나의 {@link io.openkache.core.runtime.OwnerWorker}가 소유하고, 그
 *       worker의 thread에서만 변경한다. 상태 변경에 lock을 쓰지 않는다.
 *   <li><b>EventLoop와 worker 분리</b>: Netty EventLoop는 I/O와 frame 해석만 하고, 요청은
 *       {@link io.openkache.core.runtime.Mailbox}로 owner worker에 넘긴다. EventLoop에서 blocking I/O,
 *       {@code Future.get()}, {@code CompletableFuture.join()}을 호출하지 않는다.
 *   <li><b>크기 제한 mailbox</b>: mailbox가 가득 차면 대기하지 않고 즉시 {@code BUSY}로 응답한다.
 *   <li><b>snapshot</b>: 다른 thread에 보여 주는 상태는 불변 snapshot으로 복사해 넘긴다.
 *   <li><b>close</b>: 모든 {@code close()}는 여러 번 호출해도 안전해야 한다.
 * </ul>
 *
 * <p>scheduler나 actor framework는 두지 않는다. timer 구현과 worker 배정 규칙은 실제 엔진을 구현할 때 정한다.
 */
package io.openkache.core.runtime;
