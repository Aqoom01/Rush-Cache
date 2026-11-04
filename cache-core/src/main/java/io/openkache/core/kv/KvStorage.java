package io.openkache.core.kv;

import java.nio.file.Path;
import java.util.Objects;

/**
 * {@link KvEngine}이 worker마다 쓰는 저장소 종류. 설계는 {@code docs/storage-nvme.md}.
 *
 * <p>불변이다. 어느 저장소든 {@link KvEngine}의 의미(상태값, version 규칙)는 같다. 다른 점은 {@link Nvme}의 축출뿐이다.
 */
public sealed interface KvStorage {

    /** worker별 {@code HashMap}. 축출이 없다. */
    static KvStorage memory() {
        return new Memory();
    }

    /** 메모리 저장소. */
    record Memory() implements KvStorage {}

    /**
     * NVMe 저장소: worker마다 고정 크기 segment 로그 파일과 메모리 색인을 둔다.
     *
     * <p>용량이 차면 가장 오래 전에 쓴 segment를 통째로 버리므로, SET했던 key도 나중에 GET이 MISS가 될 수 있다. SET은 쓰기
     * 버퍼에 들어가면 성공이고 fsync하지 않는다. 시작할 때 {@code path} 아래의 이전 파일을 지운다(재시작 복구 없음).
     *
     * @param path segment 파일을 둘 디렉터리. worker마다 {@code worker-<i>} 하위 디렉터리를 쓴다
     * @param capacityBytes 노드 전체 용량(byte). worker 수로 나눈 몫이 worker당 segment 2개 이상이어야 한다
     * @param segmentBytes segment 파일 하나의 크기(byte). 파일 시스템 블록 크기의 배수
     * @param writeBufferBytes worker당 쓰기 버퍼 크기(byte). 블록 크기의 배수이고 {@code segmentBytes} 이하
     * @param directIo {@code true}면 page cache를 거치지 않는다(O_DIRECT). 지원하지 않는 파일 시스템의 테스트에만 끈다
     */
    record Nvme(Path path, long capacityBytes, long segmentBytes, int writeBufferBytes, boolean directIo)
            implements KvStorage {

        public Nvme {
            Objects.requireNonNull(path, "path");
            if (segmentBytes <= 0 || writeBufferBytes <= 0 || capacityBytes <= 0) {
                throw new IllegalArgumentException("storage sizes must be positive");
            }
            if (writeBufferBytes > segmentBytes) {
                throw new IllegalArgumentException("writeBufferBytes must be <= segmentBytes");
            }
        }

        /** worker 하나가 쓸 수 있는 segment 수. 2 미만이면 이 설정으로는 {@code workers}개를 띄울 수 없다. */
        public long segmentsPerWorker(int workers) {
            return capacityBytes / workers / segmentBytes;
        }
    }
}
