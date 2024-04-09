import kademlia.KademliaNode;
import kademlia.NodeReference;
import kademlia.Util;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static kademlia.Util.*;
import static org.junit.jupiter.api.Assertions.*;

public class PutTest extends BaseTest {

    // TODO:
    // ~~ 1. Test put non-ovelapping regions ~~
    // 2. Test put + join
    // 3. Test put + leave

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
    public void testPut_nonOverlapping() throws IOException {
        List<NodeReference> lowRange = new ArrayList<>();
        List<NodeReference> highRange = new ArrayList<>();

        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15));
        lowRange.add(bootstrap.getNodeReference());
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        // low nodes
        for (int i = 1; i <= K; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(15).add(BigInteger.valueOf(i)));
            joiner.join(bootstrap.getNodeReference());
            lowRange.add(joiner.getNodeReference());
            runningNodes.add(joiner);
        }

        // high nodes
        for (int i = 0; i < K; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19).add(BigInteger.valueOf(i)));
            joiner.join(bootstrap.getNodeReference());
            highRange.add(joiner.getNodeReference());
            runningNodes.add(joiner);
        }

        String lowKey = "key3"; // keyHash=885733 ∈ [2^19, 2^20)
        String highKey = "key1"; // keyhash= 40604 ∈ [2^15, 2^16)

        bootstrap.put(highKey, "highKey");

        bootstrap.put(lowKey, "lowKey");

        List<Pair> expectHighRange = bootstrap.get("key1");
        List<Pair> expectLowRange = bootstrap.get("key3");


        expectLowRange.forEach(p -> assertTrue(lowRange.contains(p.node)));
        expectHighRange.forEach(p -> assertTrue(highRange.contains(p.node)));

        for (int i = 1; i < K+1; i++) {
            assertEquals("lowKey", runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
        }
        for (int i = K+1; i < 2*K+1; i++) {
            assertEquals("highKey", runningNodes.get(i).getLocalData().get(Util.getId(highKey)));
            assertNull(runningNodes.get(i).getLocalData().get(Util.getId(lowKey)));
        }

        assertNull(bootstrap.getLocalData().get(Util.getId(highKey)));
        assertNull(bootstrap.getLocalData().get(Util.getId(lowKey)));
    }

}
