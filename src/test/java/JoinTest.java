import kademlia.KademliaNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class JoinTest {

    static int BASE_PORT = 10_000;
    static String LOCAL_IP = "localhost";
    int BITS = 20;
    int K = 4;
    int ALPHA = 3;

    @BeforeEach
    public void init() {
        KademliaNode.setAlpha(ALPHA);
        KademliaNode.setK(K);
        KademliaNode.setIdLength(BITS);
    }

    @Test
    public void testVerySimpleJoin() throws IOException {
        KademliaNode.setAlpha(3);
        KademliaNode.setK(4);
        KademliaNode.setIdLength(20);

        KademliaNode boostrap = new KademliaNode(LOCAL_IP, BASE_PORT++);
        boostrap.initKademlia();

        // bootstrap falls into joiner's last k-bucket -> there is no refresh done
        KademliaNode joiner = new KademliaNode(LOCAL_IP, BASE_PORT++);
        joiner.join(boostrap.getNodeReference());
    }

}
