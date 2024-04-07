import kademlia.KademliaNode;
import kademlia.NodeReference;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DropNodeTest extends BaseTest {

    /**
     * If there is more than K nodes that were inserted into a bucket, it drops the first inserted
     */
    @Test
    public void testKBucketDropsNode() throws IOException {
        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        NodeReference prev = bootstrap.getNodeReference();

        int couple = 12;

        // node 1005 drop node 1008 after 1012 join
        for (int i = 1; i < couple; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger(""+i));
            runningNodes.add(joiner);
            joiner.join(prev);
            prev = joiner.getNodeReference();
        }

        assertEquals(4, runningNodes.get(5).getRoutingTable().getKBucket(3).getSize());
        assertTrue(runningNodes.get(5).getRoutingTable().getKBucket(3).contains(runningNodes.get(8).getNodeReference()));

        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("12"));
        runningNodes.add(joiner);
        joiner.join(prev);

        assertEquals(4, runningNodes.get(5).getRoutingTable().getKBucket(3).getSize());
        assertFalse(runningNodes.get(5).getRoutingTable().getKBucket(3).contains(runningNodes.get(8).getNodeReference()));
        assertTrue(runningNodes.get(5).getRoutingTable().getKBucket(3).contains(runningNodes.get(12).getNodeReference()));
    }
}
