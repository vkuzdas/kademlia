package integration;

import kademlia.KademliaNode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BigJoinTest extends IntegrationBaseTest {

    @Test
    public void testPutGet50() throws IOException {
        BITS = 160;
        KademliaNode.setIdLength(BITS);

        for (int i = 0; i < 50; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++);
            if (runningNodes.isEmpty())
                joiner.initKademlia();
            else
                joiner.join(getRandomRunningNode().getNodeReference());
            runningNodes.add(joiner);
        }
        for (int i = 0; i < 50; i++) {
            getRandomRunningNode().put("key_"+i,"val_"+i);
        }
        for (int i = 0; i < 50; i++) {
            assertEquals("val_"+i, getRandomRunningNode().get("key_"+i));
        }
    }

    @Test
    @Disabled
    public void test50() throws IOException {
        BITS = 160;
        KademliaNode.setIdLength(BITS);

        for (int i = 0; i < 50; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++);
            if (runningNodes.isEmpty())
                joiner.initKademlia();
            else
                joiner.join(getRandomRunningNode().getNodeReference());
            runningNodes.add(joiner);
        }
        for (int i = 0; i < 50; i++) {
            getRandomRunningNode().put("key_"+i,"val_"+i);
        }
        for (int i = 0; i < 50; i++) {
            assertEquals("val_"+i, getRandomRunningNode().get("key_"+i));
        }

        for (int i = 0; i < 50; i++) {
            getRandomRunningNode().delete("key_"+i);
            // TODO: original publisher & expire (as seen with get, it does not always find all of the storing nodes)
        }
    }
}
