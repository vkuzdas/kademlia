import kademlia.KademliaNode;
import kademlia.NodeReference;
import kademlia.RoutingTable;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoutingTableTest {

    private static int BASE_PORT = 10_000;
    private static String LOCAL_IP = "localhost";


    // 000001(1)
    // 000100(4)
    // 000101(5)
    // 000110(6)
    // 001100(12)
    // 001101(13)
    // 001111(15)
    // 010000(16)
    // 010010(18)
    // 010011(19)
    // 100010(34)
    // 101011(43)
    // 110001(49)
    // 111100(60)
    public void testRoutingTableInsert() {
        KademliaNode.setAlpha(3);
        KademliaNode.setK(4);
        KademliaNode.setIdLength(6);

        NodeReference owner = new NodeReference(LOCAL_IP, BASE_PORT);
        RoutingTable routingTable = new RoutingTable(6, 3, 4, owner);

        routingTable.insert(new NodeReference(LOCAL_IP, BASE_PORT + 1));
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
