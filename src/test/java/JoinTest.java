import kademlia.KademliaNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;

public class JoinTest {

    static int BASE_PORT = 10_000;
    static String LOCAL_IP = "localhost";
    int BITS = 20;
    int K = 4;
    int ALPHA = 3;

    protected final ArrayList<KademliaNode> runningNodes = new ArrayList<>();


    @BeforeEach
    public void init() {
        KademliaNode.setAlpha(ALPHA);
        KademliaNode.setK(K);
        KademliaNode.setIdLength(BITS);
    }

    @AfterEach
    public void tearDown() {
        for (KademliaNode node : runningNodes) {
            node.shutdownKademliaNode();
        }
        runningNodes.clear();
    }


    @Test
    public void testJoin_noRefresh() throws IOException {
        KademliaNode.setAlpha(3);
        KademliaNode.setK(4);
        KademliaNode.setIdLength(20);

        KademliaNode boostrap = new KademliaNode(LOCAL_IP, BASE_PORT++, new BigInteger("2").pow(19));
        boostrap.initKademlia();

        // bootstrap falls into joiner's last k-bucket -> there is no refresh done
        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ONE);
        joiner.join(boostrap.getNodeReference());
    }

    @Test
    @Disabled
    public void testJoin_refreshAllBuckets() throws IOException {
        KademliaNode.setAlpha(3);
        KademliaNode.setK(4);
        KademliaNode.setIdLength(20);

        KademliaNode boostrap = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ZERO);
        runningNodes.add(boostrap);
        boostrap.initKademlia();

        // bootstrap falls into joiner's last k-bucket -> there is no refresh done
        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, BigInteger.ONE);
        runningNodes.add(joiner);
        joiner.join(boostrap.getNodeReference());
    }

}
