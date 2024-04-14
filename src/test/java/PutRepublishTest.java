import kademlia.KademliaNode;
import kademlia.Util;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * To ensure the persistence of key-value pairs, nodes must periodically republish
 * keys. Otherwise, two phenomena may cause lookups for valid keys to fail. First, some of the k
 * nodes that initially get a key-value pair when it is published may leave the network. Second,
 * new nodes may join the network with IDs closer to some published key than the nodes on which
 * the key-value pair was originally published.
 */
public class PutRepublishTest extends BaseTest {

    private final Duration republishInterval = Duration.ofSeconds(3);

    @BeforeEach
    @Override
    public void init(TestInfo testInfo) {
        logger.warn(System.lineSeparator() + System.lineSeparator()+ "============== {} =============" + System.lineSeparator(), testInfo.getDisplayName());

        BITS = 10;

        KademliaNode.setAlpha(ALPHA);
        KademliaNode.setK(K);
        KademliaNode.setIdLength(BITS);

        KademliaNode.setRepublishInterval(republishInterval);
        KademliaNode.setDesynchronizeRepublishInterval(true);
    }

    /**
     * <i>"New nodes may join the network with IDs closer to some published key than the nodes on which
     * the key-value pair was originally published"</i> <br><br>
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

        await().atMost(interval.toMillis(), TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertEquals("value", joiner.getLocalData().get(Util.getId("key")));
                });
    }



    /**
     * <i>"Some of the k nodes that initially get a key-value pair when it is published may leave the network."</i> <br><br>
     * Validate that key is republished on K closest nodes after specified republish interval <br>
     * 1. initialize with K+1 nodes <br>
     * 2. put key on it <br>
     * 3. simulate fail on one of the storing node <br>
     * 4. wait for republish interval <br>
     * 5. validate that key is on K XOR-closest nodes <br>
     */
    @Test
    public void testPutLeaveRepublish() throws IOException {
        for (int i = 0; i < K+1; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.valueOf(i));
            if (runningNodes.isEmpty())
                joiner.initKademlia();
            else
                joiner.join(getRandomRunningNode().getNodeReference());
            runningNodes.add(joiner);
        }
        getRandomRunningNode().put("key", "value");

        // the first node (id=0) should be the farthest and therefore not contain the key initially
        assertNull(runningNodes.get(0).getLocalData().get(Util.getId("key")));

        // simulate fail on one of the storing nodes
        runningNodes.get(1).shutdownKademliaNode();
        runningNodes.remove(1);

        // after the republish interval, key should be present on the first
        // node since it is now globally one of the K XOR-closest nodes
        await().atMost((long) (1.5*republishInterval.toMillis()), TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertNotNull(runningNodes.get(0).getLocalData().get(Util.getId("key"))));
    }

    /**
     * If node fails to respond to multicastFindNode, it should be removed from routing table
     */
    @Test
    public void testMulticastFindNode_fail() throws IOException {
        KademliaNode boostrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.valueOf(0));
        boostrap.initKademlia();
        runningNodes.add(boostrap);

        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.valueOf(1));
        joiner.join(boostrap.getNodeReference());
        runningNodes.add(joiner);

        int index = boostrap.getRoutingTable().getBucketIndex(joiner.getNodeReference().getId());
        assertTrue(boostrap.getRoutingTable().getKBucket(index).contains(joiner.getNodeReference()));

        joiner.shutdownKademliaNode();
        runningNodes.remove(joiner);

        // join triggers nodeLookup and multicastFindNode, when joiner does not respond, it should be removed from bootstraps routing table
        KademliaNode joiner2 = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.valueOf(2));
        joiner2.join(boostrap.getNodeReference());
        runningNodes.add(joiner2);

        assertFalse(boostrap.getRoutingTable().getKBucket(index).contains(joiner2.getNodeReference()));
    }
}
