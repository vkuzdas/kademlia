package kademlia;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class KademliaNode {

    private static final Logger logger = LoggerFactory.getLogger(KademliaNode.class);

    private final NodeReference self;

    /**
     * Contains {@link kademlia.KademliaNode#ID_LENGTH} number of {@link kademlia.KBucket}s
     */
    private final RoutingTable routingTable;

    /**
     * Determines the length of the ID in bits as well as number of {@link kademlia.KBucket}s a single node has
     */
    private static int ID_LENGTH = 6;

    /**
     * Number of concurrent lookups
     */
    private static int ALPHA_PARAMETER = 3;

    /**
     * Max size of {@link kademlia.KBucket} and replication parameter
     */
    private static int K_PARAMETER = 4;

    public KademliaNode(String ip, int port) {
        this.self = new NodeReference(ip, port);
        this.routingTable = new RoutingTable(ID_LENGTH, ALPHA_PARAMETER, K_PARAMETER, self);
    }

    public static int getIdLength() {
        return ID_LENGTH;
    }

    public static void setIdLength(int idLength) {
        if (idLength < 4 || idLength > 160) {
            throw new IllegalArgumentException("ID length must be between 4 and 160 bits");
        }
        ID_LENGTH = idLength;
    }

    public static int getAlpha() {
        return ALPHA_PARAMETER;
    }

    public static void setAlpha(int alpha) {
        if (alpha < 1 || alpha > 10) {
            throw new IllegalArgumentException("Alpha must be between 1 and 10");
        }
        KademliaNode.ALPHA_PARAMETER = alpha;
    }

    public static int getK() {
        return K_PARAMETER;
    }

    public static void setK(int k) {
        if (k < 1 || k > 10) {
            throw new IllegalArgumentException("K must be between 1 and 10");
        }
        KademliaNode.K_PARAMETER = k;
    }
}
