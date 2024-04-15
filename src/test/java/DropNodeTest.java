import kademlia.KBucket;
import kademlia.KademliaNode;
import kademlia.NodeReference;
import org.junit.jupiter.api.Disabled;
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
    @Disabled("Does not work in github CI for some reason")
    // TODO: Fix
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


    /**
     * Validate that after joining K+1 nodes from the same bucket, a node is dropped <br> <br>
     * <i>Note: Exact dropped node cannot be validated since nodelookup is done asynchronously -> nodes respond in random, therefore are inserted into routing table at random</i>
     */
    @Test
    public void testKBucketDropsNode_simple() throws IOException {
        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        BigInteger thirdBucketStartId = new BigInteger("2").pow(3);

        // fill bucket with K nodes
        for (int i = 0; i < K; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, thirdBucketStartId.add(BigInteger.valueOf(i)));
            runningNodes.add(joiner);
            joiner.join(bootstrap.getNodeReference());
        }

        int fullBucketIndex = bootstrap.getRoutingTable().getBucketIndex(thirdBucketStartId);
        assertEquals(K, bootstrap.getRoutingTable().getKBucket(fullBucketIndex).getSize());

        // insert K+1-th node, thus dropping a node
        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, thirdBucketStartId.add(BigInteger.valueOf(K)));
        runningNodes.add(joiner);
        joiner.join(bootstrap.getNodeReference());

        // still K -> a node was dropped
        assertEquals(K, bootstrap.getRoutingTable().getKBucket(fullBucketIndex).getSize());
    }

}
