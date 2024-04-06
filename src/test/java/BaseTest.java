import kademlia.KademliaNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;

public class BaseTest {
    protected static int BASE_PORT = 10_000;
    protected static String LOCAL_IP = "localhost";
    protected int BITS = 20;
    protected int K = 4;
    protected int ALPHA = 3;

    protected final ArrayList<KademliaNode> runningNodes = new ArrayList<>();


    @BeforeEach
    public void init() {
        KademliaNode.setAlpha(ALPHA);
        KademliaNode.setK(K);
        KademliaNode.setIdLength(BITS);
    }

    /**
     * Shutdown all running nodes in order to free UDP ports
     */
    @AfterEach
    public void tearDown() {
        for (KademliaNode node : runningNodes) {
            node.shutdownKademliaNode();
        }
        runningNodes.clear();
    }
}
