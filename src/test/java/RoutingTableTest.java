import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoutingTableTest {

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
