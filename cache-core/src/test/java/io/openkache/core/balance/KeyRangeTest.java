package io.openkache.core.balance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.Bytes;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tablet의 key 범위 {@code [start, end)}와 key 순서 규칙을 확인한다.
 *
 * <p>{@code [start, end)}는 start는 포함하고, end는 포함하지 않는다는 뜻이다.
 */
@DisplayName("KeyRange (key 범위와 key 순서)")
class KeyRangeTest {

    @Nested
    @DisplayName("key 순서 (ORDER)")
    class Order {

        @Test
        @DisplayName("바이트를 부호 없이(0-255) 비교한다: 0x7f(127) < 0x80(128). Java 기본 비교와 결과가 반대다")
        void comparesBytesAsUnsigned() {
            // given
            Bytes x7f = bytes(0x7f);
            Bytes x80 = bytes(0x80);

            // when
            int unsigned = KeyRange.ORDER.compare(x7f, x80);
            int javaSigned = Byte.compare((byte) 0x7f, (byte) 0x80);

            // then: compare(a, b)는 a가 앞이면 음수, 뒤면 양수
            assertTrue(unsigned < 0, "ORDER: 127 < 128 이므로 0x7f가 앞 (음수)");
            assertTrue(javaSigned > 0, "Java byte: 127 > -128 이므로 0x7f가 뒤 (양수). ORDER는 이렇게 동작하면 안 된다");
        }

        @Test
        @DisplayName("한쪽이 다른 쪽의 앞부분이면 짧은 쪽이 앞이다: [1, 2] < [1, 2, 0]")
        void shorterPrefixComesFirst() {
            // given
            Bytes shorter = bytes(1, 2);
            Bytes longer = bytes(1, 2, 0);

            // when
            int result = KeyRange.ORDER.compare(shorter, longer);

            // then
            assertTrue(result < 0);
        }

        @Test
        @DisplayName("내용이 같으면 같은 순서다")
        void equalContentIsEqualOrder() {
            // given
            Bytes a = bytes(1, 2);
            Bytes b = bytes(1, 2);

            // when
            int result = KeyRange.ORDER.compare(a, b);

            // then
            assertEquals(0, result);
        }
    }

    @Nested
    @DisplayName("범위 포함 (contains)")
    class Contains {

        @Test
        @DisplayName("start는 포함하고 end는 포함하지 않는다: [\"b\", \"d\")에는 b, c만 들어간다")
        void startIsInclusiveAndEndIsExclusive() {
            // given
            KeyRange range = range("b", "d");

            // when & then
            assertFalse(range.contains(utf8("a")), "start보다 앞");
            assertTrue(range.contains(utf8("b")), "start와 같음 → 포함");
            assertTrue(range.contains(utf8("c")), "사이");
            assertFalse(range.contains(utf8("d")), "end와 같음 → 제외");
        }

        @Test
        @DisplayName("끝이 없는 전체 범위는 모든 key를 포함한다")
        void unboundedRangeContainsEveryKey() {
            // given
            KeyRange all = KeyRange.all();

            // when & then: 가장 앞 key(빈 바이트열)와, 모든 바이트가 최댓값(0xff)이라 뒤쪽에 오는 key
            assertTrue(all.contains(Bytes.empty()));
            assertTrue(all.contains(bytes(0xff, 0xff)));
        }

        @Test
        @DisplayName("start가 end보다 작지 않으면 범위를 만들 수 없다")
        void startMustBeLessThanEnd() {
            // given & when & then
            assertThrows(IllegalArgumentException.class, () -> range("b", "b"), "start = end");
            assertThrows(IllegalArgumentException.class, () -> range("c", "b"), "start > end");
        }
    }

    @Nested
    @DisplayName("분할 (leftOf, rightOf, isSplittableAt)")
    class Split {

        @Test
        @DisplayName("[\"b\", \"y\")를 \"m\"에서 자르면 [\"b\", \"m\")과 [\"m\", \"y\")가 된다")
        void splitProducesAdjacentRanges() {
            // given
            KeyRange range = range("b", "y");
            Bytes splitKey = utf8("m");

            // when
            KeyRange left = range.leftOf(splitKey);
            KeyRange right = range.rightOf(splitKey);

            // then: 왼쪽 끝 = 자른 위치 = 오른쪽 시작
            assertEquals(range("b", "m"), left);
            assertEquals(range("m", "y"), right);
        }

        @Test
        @DisplayName("start에서 자르면 왼쪽이 비므로 자를 수 없다")
        void cannotSplitAtStart() {
            // given
            KeyRange range = range("b", "y");

            // when & then: 왼쪽 ["b", "b")는 빈 범위
            assertFalse(range.isSplittableAt(utf8("b")));
            assertThrows(IllegalArgumentException.class, () -> range.leftOf(utf8("b")));
        }

        @Test
        @DisplayName("범위 밖에서는 자를 수 없다")
        void cannotSplitOutsideTheRange() {
            // given
            KeyRange range = range("b", "y");

            // when & then
            assertFalse(range.isSplittableAt(utf8("a")), "start보다 앞");
            assertFalse(range.isSplittableAt(utf8("y")), "end와 같음");
            assertThrows(IllegalArgumentException.class, () -> range.rightOf(utf8("z")));
        }
    }

    // ---- 도우미 ----

    /** 0-255 정수로 바이트열을 만든다. 예: {@code bytes(0x7f, 0x80)}. */
    private static Bytes bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return Bytes.copyOf(result);
    }

    private static Bytes utf8(String text) {
        return Bytes.utf8(text);
    }

    /** 양 끝이 있는 범위 {@code [start, end)}. */
    private static KeyRange range(String start, String end) {
        return new KeyRange(Optional.of(utf8(start)), Optional.of(utf8(end)));
    }
}
