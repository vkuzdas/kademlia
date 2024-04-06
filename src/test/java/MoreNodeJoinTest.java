import kademlia.KademliaNode;
import kademlia.NodeReference;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MoreNodeJoinTest extends BaseTest {
    @Test
    public void testThree_joinBootstrap() throws IOException {
        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        KademliaNode joiner1 = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ONE);
        runningNodes.add(joiner1);
        joiner1.join(bootstrap.getNodeReference());

        KademliaNode joiner2 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2"));
        runningNodes.add(joiner2);
        joiner2.join(bootstrap.getNodeReference());

        assertEquals(2, bootstrap.getRoutingTable().getSize());
        assertEquals(2, joiner1.getRoutingTable().getSize());
        assertEquals(2, joiner2.getRoutingTable().getSize());

        assertEquals(2, countAllRoutinTableEntries(bootstrap));
        assertEquals(2, countAllRoutinTableEntries(joiner1));
        assertEquals(2, countAllRoutinTableEntries(joiner2));
    }


    @Test
    public void testThree_joinChain() throws IOException {
        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        KademliaNode joiner1 = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ONE);
        runningNodes.add(joiner1);
        joiner1.join(bootstrap.getNodeReference());

        KademliaNode joiner2 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2"));
        runningNodes.add(joiner2);
        joiner2.join(joiner1.getNodeReference());


        assertEquals(2, bootstrap.getRoutingTable().getSize());
        assertEquals(2, joiner1.getRoutingTable().getSize());
        assertEquals(2, joiner2.getRoutingTable().getSize());

        assertEquals(2, countAllRoutinTableEntries(bootstrap));
        assertEquals(2, countAllRoutinTableEntries(joiner1));
        assertEquals(2, countAllRoutinTableEntries(joiner2));
    }

    @Test
    public void testFour_joinBootstrap() throws IOException {
        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        int couple = 4;

        for (int i = 1; i < couple; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger(""+i));
            runningNodes.add(joiner);
            joiner.join(bootstrap.getNodeReference());
        }

        for(KademliaNode node : runningNodes) {
            assertEquals(couple-1, node.getRoutingTable().getSize());
            assertEquals(couple-1, countAllRoutinTableEntries(node));
        }
    }

//    // FIXME
//    @Test
//    public void testFour_joinChained() throws IOException {
//        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("0"));
//        runningNodes.add(bootstrap);
//        bootstrap.initKademlia();
//
//        KademliaNode joiner1 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("1"));
//        runningNodes.add(joiner1);
//        joiner1.join(bootstrap.getNodeReference());
//
//        KademliaNode joiner2 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2"));
//        runningNodes.add(joiner2);
//        joiner2.join(joiner1.getNodeReference());
//
//        KademliaNode joiner3 = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("3"));
//        runningNodes.add(joiner3);
//        joiner3.join(joiner2.getNodeReference());
//
//
//        for(KademliaNode node : runningNodes) {
//            assertEquals(3, node.getRoutingTable().getSize());
//            assertEquals(3, countAllRoutinTableEntries(node));
//        }
//    }
//
//
//    // FIXME
//    @Test
//    public void testFive_joinBoostrap() throws IOException {
//        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
//        runningNodes.add(bootstrap);
//        bootstrap.initKademlia();
//
//        int couple = 5;
//
//        for (int i = 1; i < couple; i++) {
//            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger(""+i));
//            runningNodes.add(joiner);
//            joiner.join(bootstrap.getNodeReference());
//        }
//    }


    /**
     * Since im not very confident about {@link kademlia.RoutingTable#getSize()} method
     */
    private int countAllRoutinTableEntries(KademliaNode node) {
        int cnt = 0;
        for (int i = 0; i < BITS; i++) {
            cnt += node.getRoutingTable().getKBucket(i).getSize();
        }
        return cnt;
    }
}
