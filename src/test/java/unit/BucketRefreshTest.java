package unit;

import kademlia.KademliaNode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

public class BucketRefreshTest extends BaseTest {

    @Test
    @Disabled("run manually")
    public void testRefreshBucket() throws IOException, InterruptedException {
        KademliaNode.setRefreshInterval(Duration.ofSeconds(5));
        BITS = 60;
        KademliaNode.setIdLength(BITS);

        for (int i = 0; i < 5; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++);
            if (runningNodes.isEmpty())
                joiner.initKademlia();
            else
                joiner.join(getRandomRunningNode().getNodeReference());
            runningNodes.add(joiner);
        }

        Thread.sleep(1000*60);
    }
}
