/**
 * {@code hash-tag-tablet} variant: key의 hash tag(중괄호로 감싼 부분)만 해시해 같은 tag의 key를 한 Tablet에 모은다. Redis Cluster
 * hash tag, DynamoDB·Cassandra partition key 계열.
 *
 * <p>{@link io.openkache.core.tablet.tag.HashTagScheme} 하나다. 표와 노드 증감은 {@code range}(hash-range-tablet)에 위임하고
 * {@code keyHash}만 다르다.
 */
package io.openkache.core.tablet.tag;
