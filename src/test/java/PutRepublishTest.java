import kademlia.KademliaNode;
import kademlia.Util;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;

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
    public void init() {
        BITS = 10;

        KademliaNode.setAlpha(ALPHA);
        KademliaNode.setK(K);
        KademliaNode.setIdLength(BITS);

        KademliaNode.setRepublishInterval(republishInterval);
    }


     /*TODO: opt-1 & opt-2:
        opt-1: pokud node obdrzi STORE, potom nebude dalsi hodinu publikovat
            -> This ensures that >>>as long as republication intervals are not exactly synchronized<<<, only    //one node will republish a given key-value pair every hour
        opt-2: pokud jsme delali uplynulou hodinu bucket refresh, nebudeme delat FIND_NODE ale rovnout STORE*/


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

        assertEquals("value", joiner.getLocalData().get(Util.getId("key")));
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
    @Disabled("TODO")
    public void testPutLeaveRepublish() throws IOException, InterruptedException {
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

        // TODO: onError should be removed from kBucket

        // wait for a bit longer than republish interval (not doing so may trigger testing node shutdown method before republish)
        Thread.sleep((long) (1.5*republishInterval.toMillis()));

        // after the republish interval, key should be present on the first
        // node since it is now globally one of the K XOR-closest nodes
        assertNotNull(runningNodes.get(0).getLocalData().get(Util.getId("key")));
    }

    /**
     * When republishing to left node, it gets deleted when no response arrives
     */
    @Test
    @Disabled("TODO")
    public void testLeaveRepublish() throws IOException, InterruptedException {
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

        // TODO: onError should be removed from kBucket

        // wait for a bit longer than republish interval (not doing so may trigger testing node shutdown method before republish)
        Thread.sleep((long) (1.5*republishInterval.toMillis()));
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
