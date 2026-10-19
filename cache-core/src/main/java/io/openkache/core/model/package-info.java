/**
 * 모든 연구가 공유하는 식별자와 값 타입.
 *
 * <p>이 패키지의 타입은 모두 불변이며 여러 thread에서 동기화 없이 공유할 수 있다. 식별자는 숫자를 감싼 record로 두어
 * {@code NodeId}와 {@code TabletId}처럼 의미가 다른 값을 섞어 쓰지 못하게 한다.
 */
package io.openkache.core.model;
