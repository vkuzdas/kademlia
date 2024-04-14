import kademlia.KademliaNode;
import kademlia.NodeReference;
import kademlia.Util;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static kademlia.Util.*;
import static org.junit.jupiter.api.Assertions.*;

public class PutTest extends BaseTest {


    @Test
    public void testPut_singleNode() throws IOException {
        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        bootstrap.put("key1", "val1");
        assertEquals(1, bootstrap.getLocalData().size());

        List<Pair> pairs = bootstrap.get("key1");
        assertEquals(1, pairs.size());
        assertEquals("val1", pairs.get(0).value);
        assertEquals(bootstrap.getNodeReference(), pairs.get(0).node);
    }

    @Test
    public void testPut_twoNode() throws IOException {
        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ONE);
        runningNodes.add(joiner);
        joiner.join(bootstrap.getNodeReference());

        bootstrap.put("key1", "val1");

        List<Pair> pairs = joiner.get("key1");
        assertEquals(2, pairs.size());
    }

    @Test
    public void testPut_nonOverlapping_random() throws IOException {
        KademliaNode.setIdLength(10);
        BITS = 10;
        String lowKey = "key4";  // keyHash= 114 ∈ [2^6, 2^7)
        String highKey = "key7"; // keyhash= 696 ∈ [2^9, 2^10)

        List<NodeReference> lowRange = new ArrayList<>();
        List<NodeReference> highRange = new ArrayList<>();

        // place nodes around lowKey
        for (int i = 0; i < K; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, Util.getId(lowKey).add(BigInteger.valueOf(i+1)));

            if (runningNodes.isEmpty()) {
                joiner.initKademlia();
            } else {
                joiner.join(getRandomRunningNode().getNodeReference());
            }

            lowRange.add(joiner.getNodeReference());
            runningNodes.add(joiner);
        }

        // place nodes around highKey
        for (int i = 0; i < K; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, Util.getId(highKey).add(BigInteger.valueOf(i+1)));

            joiner.join(getRandomRunningNode().getNodeReference());

            highRange.add(joiner.getNodeReference());
            runningNodes.add(joiner);
        }

        // no matter the chosen node, key should be placed onto appropriate nodes
        getRandomRunningNode().put(highKey, "highValue");
        getRandomRunningNode().put(lowKey, "lowValue");

        // no matter the chosen node, it should return nodes placed in appropriate distance from polled key
        List<Pair> expectHighRange = getRandomRunningNode().get(highKey);
        List<Pair> expectLowRange = getRandomRunningNode().get(lowKey);



        expectLowRange.forEach(p -> {
            assertTrue(lowRange.contains(p.node));
            assertEquals("lowValue", p.value);
        });
        expectHighRange.forEach(p -> {
            assertTrue(highRange.contains(p.node));
            assertEquals("highValue", p.value);
        });

        for (int i = 0; i < K; i++) {
            assertEquals("lowValue", runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
        }
        for (int i = K; i < 2*K; i++) {
            assertEquals("highValue", runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
        }
    }

}
