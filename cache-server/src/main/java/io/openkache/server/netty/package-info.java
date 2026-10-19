/**
 * Netty pipeline과 wire 프로토콜 v1 codec.
 *
 * <p>{@link io.openkache.server.netty.Frame}은 server 내부 wire 타입이며 core에 노출하지 않는다. Netty
 * {@code Channel}과 {@code ByteBuf}는 이 패키지 밖으로 나가지 않는다. handler는 EventLoop에서 실행되므로 block하지 않는다.
 */
package io.openkache.server.netty;
