import kademlia.KademliaNode;
import kademlia.Util;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static kademlia.Util.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

}
