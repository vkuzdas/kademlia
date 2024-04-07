import kademlia.KBucket;
import kademlia.KademliaNode;
import kademlia.NodeReference;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RandomJoinTest extends BaseTest {

    @Test
    public void testRandomJoin_32() throws IOException {
        ArrayList<Integer> ids_32 = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            ids_32.add(i);
        }

        KademliaNode bootstrap = new KademliaNode(LOCAL_IP, BASE_PORT++, getRandomUniqueId(ids_32));
        runningNodes.add(bootstrap);
        bootstrap.initKademlia();

        while(!ids_32.isEmpty()) {
            KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++, getRandomUniqueId(ids_32));
            NodeReference toJoin = runningNodes.get(new Random().nextInt(runningNodes.size())).getNodeReference();
            joiner.join(toJoin);
            runningNodes.add(joiner);
        }

    }

    private BigInteger getRandomUniqueId(List<Integer> ids) {
        int i = new Random().nextInt(ids.size());
        return BigInteger.valueOf(ids.remove(i));
    }

}
