package io.openkache.core.tablet.prefix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.DistributionSchemeContract;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import org.junit.jupiter.api.Test;

class PrefixHashSchemeTest extends DistributionSchemeContract {

    private static final int T = 8;

    @Override
    protected DistributionScheme scheme() {
        return new PrefixHashScheme(T);
    }

    @Test
    void tagIsTheFirstBracePairWhenNotEmpty() {
        assertEquals(Bytes.utf8("user1"), PrefixHashScheme.tagOf(Bytes.utf8("{user1}:profile")));
        assertEquals(Bytes.utf8("user1"), PrefixHashScheme.tagOf(Bytes.utf8("{user1}:{x}")));
        assertEquals(Bytes.utf8("{user1"), PrefixHashScheme.tagOf(Bytes.utf8("{{user1}}")));
        assertEquals(Bytes.utf8("user1"), PrefixHashScheme.tagOf(Bytes.utf8("a:{user1}")));
        assertEquals(Bytes.utf8("u"), PrefixHashScheme.tagOf(Bytes.utf8("{u}")));
    }

    @Test
    void wholeKeyWhenThereIsNoUsableTag() {
        for (String key : new String[] {"user1", "{}user1", "{user1", "user1}", "}{", "", "{}", "{"}) {
            assertEquals(Bytes.utf8(key), PrefixHashScheme.tagOf(Bytes.utf8(key)), key);
        }
    }

    @Test
    void keysWithTheSameTagHashTheSameAsTheBareTag() {
        DistributionScheme s = scheme();
        long tag = KeyHash.of(Bytes.utf8("user1"));

        assertEquals(tag, s.keyHash(Bytes.utf8("{user1}:profile")));
        assertEquals(tag, s.keyHash(Bytes.utf8("{user1}:cart:item:42")));
        assertEquals(tag, s.keyHash(Bytes.utf8("user1")));
        assertNotEquals(tag, s.keyHash(Bytes.utf8("{user2}:profile")));
        assertEquals(KeyHash.of(Bytes.utf8("plain-key")), s.keyHash(Bytes.utf8("plain-key")));
    }

    @Test
    void keysWithTheSameTagAlwaysLandInOneTablet() {
        DistributionScheme s = scheme();
        RoutingTable t = s.initial(nodes(4));
        for (int tag = 0; tag < 200; tag++) {
            Tablet first = t.lookup(s.keyHash(Bytes.utf8("{t" + tag + "}:0")));
            for (int i = 1; i < 20; i++) {
                assertEquals(first, t.lookup(s.keyHash(Bytes.utf8("{t" + tag + "}:" + i))), "tag " + tag);
            }
        }
    }

    @Test
    void tableAndPlansAreExactlyHashRangeTablet() {
        DistributionScheme s = scheme();
        HashRangeScheme range = new HashRangeScheme(T);
        RoutingTable a = s.initial(nodes(5));
        RoutingTable b = range.initial(nodes(5));
        assertEquals(b.tablets(), a.tablets());

        Plan addA = s.onNodeAdded(a, new NodeId(6));
        Plan addB = range.onNodeAdded(b, new NodeId(6));
        assertEquals(addB.prepared().tablets(), addA.prepared().tablets());
        assertEquals(addB.moves(), addA.moves());

        Plan removeA = s.onNodeRemoved(a, new NodeId(2));
        Plan removeB = range.onNodeRemoved(b, new NodeId(2));
        assertEquals(removeB.prepared().tablets(), removeA.prepared().tablets());
        assertEquals(removeB.moves(), removeA.moves());

        assertEquals(range.metadataBytes(a), s.metadataBytes(a));
        assertTrue(s.supportsSplit());
        assertTrue(s.supportsMove());
    }

    @Test
    void nameAndTabletsPerNode() {
        assertEquals("prefix-hash-tablet", scheme().name());
        assertEquals(T, new PrefixHashScheme(T).tabletsPerNode());
        assertEquals(HashRangeScheme.DEFAULT_TABLETS_PER_NODE, new PrefixHashScheme().tabletsPerNode());
    }
}
