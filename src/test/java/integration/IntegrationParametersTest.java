package integration;

import kademlia.KademliaNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unit.BaseTest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;

public class IntegrationParametersTest extends IntegrationTest {
    protected static final Logger logger = LoggerFactory.getLogger(BaseTest.class);
    protected static int BASE_PORT = 11_000;
    protected static String LOCAL_IP = "localhost";
    protected int BITS = 160;
    protected int K = 10;
    protected int ALPHA = 7;
    protected final Random random = new Random();

    protected final ArrayList<KademliaNode> runningNodes = new ArrayList<>();


    @BeforeEach
    public void init(TestInfo testInfo) {
        logger.warn(System.lineSeparator() + System.lineSeparator()+ "============== {} =============" + System.lineSeparator(), testInfo.getDisplayName());
        KademliaNode.setAlpha(ALPHA);
        KademliaNode.setK(K);
        KademliaNode.setIdLength(BITS);
        KademliaNode.setRepublishInterval(Duration.ofDays(1)); // turn off republishing
        KademliaNode.setExpireInterval(Duration.ofDays(1)); // turn off expiring
        KademliaNode.setRefreshInterval(Duration.ofDays(1)); // turn off refreshing
    }

}
