import kademlia.KademliaNode;
import kademlia.RoutingTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Random;

public class BaseTest {
    protected static final Logger logger = LoggerFactory.getLogger(BaseTest.class);
    protected static int BASE_PORT = 10_000;
    protected static String LOCAL_IP = "localhost";
    protected int BITS = 20;
    protected int K = 4;
    protected int ALPHA = 3;
    protected final Random random = new Random();

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

    protected KademliaNode getRandomRunningNode() {
        return runningNodes.get(random.nextInt(runningNodes.size()));
    }

    protected BigInteger getRandomId() {
        return new BigInteger(""+random.nextInt((int)Math.pow(2,BITS)));
    }
}
