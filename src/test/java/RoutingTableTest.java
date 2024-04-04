import kademlia.KBucket;
import kademlia.KademliaNode;
import kademlia.NodeReference;
import kademlia.RoutingTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RoutingTableTest {

    static int BASE_PORT = 10_000;
    static String LOCAL_IP = "localhost";

    int BITS = 20;
    int K = 4;
    int ALPHA = 3;


    @BeforeEach
    public void init() {
        KademliaNode.setAlpha(ALPHA);
        KademliaNode.setK(K);
        KademliaNode.setIdLength(BITS);
    }

    @Test
    public void testInsertOverflow() {
        NodeReference owner = new NodeReference(LOCAL_IP, BASE_PORT++);
        owner.setId(BigInteger.ZERO);
        RoutingTable routingTable = new RoutingTable(BITS, ALPHA, K, owner);

        int idIncrement = 16; // 16 = 2^4, therefore only 4th KBucket is in our interest
        NodeReference dropped = new NodeReference(LOCAL_IP, BASE_PORT++);
        dropped.setId(owner.getId().add(BigInteger.valueOf(idIncrement++)));
        routingTable.insert(dropped);

        // fill KBucket with K nodes so that the first node is dropped
        for (int i = 0; i < K; i++) {
            NodeReference n = new NodeReference(LOCAL_IP, BASE_PORT++);
            n.setId(owner.getId().add(BigInteger.valueOf(idIncrement++)));
            routingTable.insert(n);
        }

        int insertIndex = owner.getId().xor(BigInteger.valueOf(idIncrement)).bitLength() - 1;
        KBucket targetKbucket = routingTable.getKBucket(insertIndex);

        assertFalse(targetKbucket.contains(dropped));

    }

    @Test
    public void testRoutingTableInsert() {
        NodeReference owner = new NodeReference(LOCAL_IP, BASE_PORT++);
        RoutingTable routingTable = new RoutingTable(BITS, ALPHA, K, owner);

        for (int i = 0; i < 20; i++) {
            NodeReference n = new NodeReference(LOCAL_IP, BASE_PORT++);
            routingTable.insert(n);
            int insertIndex = owner.getId().xor(n.getId()).bitLength() - 1;

            if (owner.getId().compareTo(n.getId()) != 0) {
                assertTrue(routingTable.getKBucket(insertIndex).contains(n));
            }
        }
    }

    @Test
    public void testFindKClosest() {
        NodeReference owner = new NodeReference(LOCAL_IP, BASE_PORT++);
        RoutingTable routingTable = new RoutingTable(BITS, ALPHA, K, owner);

        ArrayList<NodeReference> inserted = new ArrayList<>();

        // insert and assert correct KBucket insertion
        for (int i = 0; i < 40; i++) {
            NodeReference n = new NodeReference(LOCAL_IP, BASE_PORT++);
            routingTable.insert(n);
            inserted.add(n);
            int insertIndex = owner.getId().xor(n.getId()).bitLength() - 1;

            if (owner.getId().compareTo(n.getId()) != 0) {
                assertTrue(routingTable.getKBucket(insertIndex).contains(n));
            }
        }

        for (int i = 0; i < 20; i++) {
            int randomId = new Random().nextInt((int)Math.pow(2, BITS));
            BigInteger targetId = new BigInteger(String.valueOf(randomId));
            List<NodeReference> kClosest = routingTable.findKClosest(targetId);

            // got some K nodes
            assertEquals(KademliaNode.getK(), kClosest.size());
        }

    }


    @Test
    public void testDistance() {
        BigInteger routingTableOwnerID = new BigInteger("60");
        BigInteger newNode = new BigInteger("49");
        String binaryDistance = routingTableOwnerID.xor(newNode).toString(2);

        assertEquals("1101", binaryDistance);

        int bucketIndex = binaryDistance.length() - 1;

        assertEquals(3, bucketIndex);
    }

    @Test
    public void testXOR() {
        // 49 XOR 60 = 13
        // 49 XOR 13 = 60

        BigInteger a = new BigInteger("49");
        BigInteger b = new BigInteger("60");

        BigInteger aXb = a.xor(b);
        assertEquals(new BigInteger("13"), aXb);

        BigInteger bXa = b.xor(a);
        assertEquals(new BigInteger("13"), bXa);

    }

}
