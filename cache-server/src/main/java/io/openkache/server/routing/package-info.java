/**
 * B1 Tablet 라우팅 실서버. 라우팅 표를 아는 노드들이 실제 TCP로 표를 주고받고,
 * 자기 Tablet이 아닌 요청을 MOVED로 돌려보낸다.
 *
 * <p>구성: {@code B1Server}(조립부), {@code B1Wire}(peer frame 변환, protocol-v1.md §6-2),
 * {@code B1NodeConfig}(고정 노드 설정), {@code B1ServerMain}(진입점).
 * node 0이 Controller를 겸한다. 인증이 없으므로 전용 보안 그룹 안에서만 사용한다.
 */
package io.openkache.server.routing;
