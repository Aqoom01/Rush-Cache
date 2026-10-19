/**
 * server와 연구 엔진 사이의 공통 API.
 *
 * <p>server는 wire frame을 {@link io.openkache.core.api.Command}와
 * {@link io.openkache.core.api.RequestContext}로 바꿔 {@link io.openkache.core.api.CacheEngine}에 넘기고, 돌려받은
 * {@link io.openkache.core.api.Result}를 다시 frame으로 바꾼다. 이 패키지는 Netty 타입(Channel, ByteBuf)을 노출하지 않는다.
 */
package io.openkache.core.api;
