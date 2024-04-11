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

    /* TODO:
       ~~ 1. Test put non-ovelapping regions ~~
       2. Test put + join
       3. Test put + leave
    */

    /**
     * Validate that key is republished on K closest nodes after specified republish interval <br>
     * 1. put a key on a Single node <br>
     * 2. put key on it <br>
     * 3. join with other nodes <br>
     * 4. wait for republish interval <br>
     * 5. validate that key is on K XOR-closest nodes <br>
     */
    @Test
    public void testPutJoinRepublish() throws IOException, InterruptedException {
        BITS = 10;
        KademliaNode.setIdLength(BITS);
        Duration interval = Duration.ofSeconds(3);
        KademliaNode.setRepublishInterval(interval);

        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        bootstrap.initKademlia();
        bootstrap.put("key", "value");

        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ONE);
        joiner.join(bootstrap.getNodeReference());

        assertNull(joiner.getLocalData().get(Util.getId("key")));

        // wait for republish interval
        Thread.sleep(interval.toMillis());

        assertEquals("value", joiner.getLocalData().get(Util.getId("key")));
    }

    /**
     * insert some 2K nodes
     * put a value on them
     * validate value is on closest nodes
     * make another node join the network
     * validate that key is reorganized on the joined node
     */
    @Test
    @Disabled("TODO")
    public void testPutJoinReorganizeKeys() throws IOException {
        BITS = 10;
        KademliaNode.setIdLength(BITS);

        for (int i = 0; i < 2*K; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, getRandomId());

            if (runningNodes.isEmpty()) {
                joiner.initKademlia();
            } else {
                joiner.join(getRandomRunningNode().getNodeReference());
            }

            runningNodes.add(joiner);
        }

        String key = "key";
        BigInteger keyHash = Util.getId(key);
        String value = "value";
        getRandomRunningNode().put(key, value);

        // K closest from all runnning
        List<KademliaNode> expectedKClosest1 = runningNodes.stream()
                .sorted(Comparator.comparing(node -> node.getNodeReference().getId().xor(keyHash)))
                .limit(K)
                .collect(Collectors.toList());

        // K closest returned from the network
        List<NodeReference> actualKClosest1 = new ArrayList<>();
        getRandomRunningNode().get(key).forEach(p -> actualKClosest1.add(p.node));

        // Assert they are same
        expectedKClosest1.forEach(n -> {
            assertTrue(actualKClosest1.contains(n.getNodeReference()));
            assertEquals(value, n.getLocalData().get(keyHash));
        });


        // TODO: when joining, new node should get XOR-closest nodes

        // new node that is closest to keyHash
        KademliaNode closeToKeyHash = new KademliaNode(LOCAL_IP, BASE_PORT++, keyHash.add(BigInteger.ONE));
        closeToKeyHash.join(getRandomRunningNode().getNodeReference());

        // K closest from all runnning
        List<KademliaNode> expectedKClosest2 = runningNodes.stream()
                .sorted(Comparator.comparing(node -> node.getNodeReference().getId().xor(keyHash)))
                .limit(K)
                .collect(Collectors.toList());

        // K closest returned from the network
        List<NodeReference> actualKClosest2 = new ArrayList<>();
        getRandomRunningNode().get(key).forEach(p -> actualKClosest2.add(p.node));

        // Assert they are same
        expectedKClosest2.forEach(n -> {
            assertTrue(actualKClosest2.contains(n.getNodeReference()));
            assertEquals(value, n.getLocalData().get(keyHash));
        });
    }

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
