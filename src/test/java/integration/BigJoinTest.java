package integration;

import kademlia.KademliaNode;
import kademlia.NodeReference;
import kademlia.Util;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Time;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
public class BigJoinTest extends IntegrationBaseTest {

    @Test
    public void testPutGetExpire50() throws IOException {
        BITS = 160;
        KademliaNode.setIdLength(BITS);
        int tExpire = 6;
        KademliaNode.setExpireInterval(Duration.ofSeconds(tExpire));
        KademliaNode.setRepublishInterval(Duration.ofDays(1)); // no republication

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

        // Expire all
        await().atMost(tExpire, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    for (int i = 0; i < 50; i++) {
                        assertEquals(0, runningNodes.get(0).getLocalData().size());
                    }
                });
    }

    @Test
    @Disabled("Put, get, delete don't return globally closest nodes. Delete therefore does not guarantee global key deletion.")
    public void test50() throws IOException {
        BITS = 20;
        KademliaNode.setIdLength(BITS);

        for (int i = 0; i < 50; i++) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++);
            if (runningNodes.isEmpty())
                joiner.initKademlia();
            else
                joiner.join(getRandomRunningNode().getNodeReference());
            runningNodes.add(joiner);
        }
        for (int i = 0; i < 100; i++) {
            getRandomRunningNode().put("key_"+i,"val_"+i);
            logger.debug("globally closest: {}", globallyXORClosest("key_"+i));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("val_"+i, getRandomRunningNode().get("key_"+i));
            logger.debug("globally closest: {}", globallyXORClosest("key_"+i));
        }
        for (int i = 0; i < 100; i++) {
            getRandomRunningNode().delete("key_"+i);
            logger.debug("globally closest: {}", globallyXORClosest("key_"+i));
        }
        runningNodes.forEach(node -> {
            if (!node.getLocalData().isEmpty()) {
                logger.error("Node "+node.getNodeReference().getId() + " has data: {}", node.getLocalData());
            }
            assertEquals(0, node.getLocalData().size(), "Node "+node.getNodeReference().getId() + " has data");
        });
    }


    private List<NodeReference> globallyXORClosest(String keyHash) {
        return runningNodes.stream()
                .map(KademliaNode::getNodeReference)
                .sorted(Comparator.comparing(node -> Util.getId(keyHash).xor(node.getId())))
                .limit(K)
                .collect(Collectors.toList());
    }
}
