package kademlia;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proto.Kademlia;
import proto.KademliaServiceGrpc;

import java.io.IOException;
import java.math.BigInteger;

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

    private final Server server;

    private KademliaServiceGrpc.KademliaServiceBlockingStub blockingStub;
    private KademliaServiceGrpc.KademliaServiceStub asyncStub;




    public KademliaNode(String ip, int port) {
        this.self = new NodeReference(ip, port);
        this.routingTable = new RoutingTable(ID_LENGTH, ALPHA_PARAMETER, K_PARAMETER, self);

        server = ServerBuilder.forPort(port)
                .addService(new KademliaNodeServer())
                .build();
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

    ////////////////////////////////
    ///  CLIENT-SIDE PROCESSING  ///
    ////////////////////////////////

    public void initKademlia() throws IOException {
        startServer();
        logger.debug("[{}]  started FIX", self);
    }

    private void startServer() throws IOException {
        server.start();
        logger.warn("[{}]  Server started, listening on {}", self, self.port);
    }

    public void shutdownKademliaNode() {
        stopServer();
    }

    public void stopServer() {
        if (server != null) {
            server.shutdownNow();
            logger.warn("Server stopped, listening on {}", self.port);
        }
    }

    /**
     * Kademlia Join
     * 	kdyz joinuje U, musi mit kontakt na boostrap W
     * 	U insertne W do spravneho KB
     * 	U potom udela lookup svojeho ID
     * 	U nakonec refreshne vsechny KB ktere jsou vzdalenejsi vice nez jeho nejblizsi neighbor
     * 		- behem refreshe U plni svoje KB a insertuje sama sebe do ostatnich KB dle potreby
     */
    public void join(NodeReference bootstrap) throws IOException {
        startServer();

        // U insertne W do spravneho KB
        routingTable.insert(bootstrap);

        // prompt W to do lookup for an ID
        //  The "self-lookup" will populate other nodes' k-buckets with the new node ID, and will populate the joining node's k-buckets with the nodes in the path between it and the bootstrap node.
        ManagedChannel ch = ManagedChannelBuilder.forTarget(bootstrap.getAddress()).usePlaintext().build();
        asyncStub = KademliaServiceGrpc.newStub(ch);
        Kademlia.LookupRequest req = Kademlia.LookupRequest.newBuilder().setTargetId(self.getId().toString()).build();
//        asyncStub.nodeLookup(req);


    }

    public void nodeLookup(BigInteger targetId) {

    }



    ////////////////////////////////
    ///  SERVER-SIDE PROCESSING  ///
    ////////////////////////////////
    
    /**
     * Server-side of Kademlia node
     */
    private class KademliaNodeServer extends KademliaServiceGrpc.KademliaServiceImplBase {

    }

}
