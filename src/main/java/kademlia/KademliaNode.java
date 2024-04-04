package kademlia;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proto.Kademlia;
import proto.KademliaServiceGrpc;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static proto.Kademlia.LookupRequest.Type.JOIN;

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
        // TODO:
        //  The "self-lookup" will populate other nodes'
        //  k-buckets with the new node ID, and will
        //  populate the joining node's k-buckets with
        //  the nodes in the path between it and the bootstrap node.
        ManagedChannel ch = ManagedChannelBuilder.forTarget(bootstrap.getAddress()).usePlaintext().build();
        blockingStub = KademliaServiceGrpc.newBlockingStub(ch);
        Kademlia.LookupRequest request = Kademlia.LookupRequest.newBuilder()
                .setTargetId(self.getId().toString())
                .setType(JOIN)
                .build();

        Kademlia.LookupResponse response = blockingStub.promptNodeLookup(request);


    }

    /**
     * Locate K globally-closest nodes to the targetId
     */
    public List<NodeReference> nodeLookup(BigInteger targetId) {

        List<NodeReference> k_best = routingTable.findKClosest(targetId);

        while (true) { // while better results are coming

            Stack<NodeReference> toQuery = new Stack<>();
            toQuery.addAll(k_best);

            int calls = Math.min(ALPHA_PARAMETER, toQuery.size());
            CountDownLatch latch = new CountDownLatch(calls);

            List<NodeReference> found = Collections.synchronizedList(new ArrayList<>());

            // query all K nodes eventually
            while (!toQuery.isEmpty()) {

                // do max. alpha concurrent calls
                for (int i = 0; i < calls; i++) {
                    NodeReference curr = toQuery.pop();

                    ManagedChannel channel = ManagedChannelBuilder.forTarget(curr.getAddress()).usePlaintext().build();
                    Kademlia.FindNodeRequest request = Kademlia.FindNodeRequest.newBuilder().setTargetId(targetId.toString()).build();
                    KademliaServiceGrpc.newStub(channel).findNode(request, new StreamObserver<Kademlia.FindNodeResponse>() {
                        @Override
                        public void onNext(Kademlia.FindNodeResponse findNodeResponse) {
                            findNodeResponse.getKClosestList().stream().map(NodeReference::new).forEach(found::add);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            // TODO: remove ?
                            logger.error("Error while finding node", throwable);
                        }

                        @Override
                        public void onCompleted() {
                            // since the response is unary, this function may be redundant
                            logger.debug("Node lookup completed");
                        }
                    });
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.error("Waiting for async calls interrupted", e);
                }
            }
            // done concurrently asking all K best nodes

            // termination check: if closer nodes were found, continue
            BigInteger currBestDist = k_best.stream()
                    .sorted(Comparator.comparing(node -> targetId.subtract(node.getId()).abs()))
                    .limit(1)
                    .map(n -> targetId.subtract(n.getId()).abs())
                    .findFirst().get();

            BigInteger newBestDist = found.stream()
                    .sorted(Comparator.comparing(node -> targetId.subtract(node.getId()).abs()))
                    .limit(1)
                    .map(n -> targetId.subtract(n.getId()).abs())
                    .findFirst().get();

            if (currBestDist.compareTo(newBestDist) > 0) {
                break;
            }

            // update k_best: remove duplicates, sort by distance to target, keep only K best
            found.addAll(k_best);

            k_best = found.stream()
                    .distinct()
                    .sorted(Comparator.comparing(node -> targetId.subtract(node.getId()).abs()))
                    .limit(K_PARAMETER)
                    .collect(Collectors.toList());
        }

        return k_best;
    }



    ////////////////////////////////
    ///  SERVER-SIDE PROCESSING  ///
    ////////////////////////////////
    
    /**
     * Server-side of Kademlia node
     */
    private class KademliaNodeServer extends KademliaServiceGrpc.KademliaServiceImplBase {


        @Override
        public void promptNodeLookup(Kademlia.LookupRequest request, StreamObserver<Kademlia.LookupResponse> responseObserver) {
            BigInteger targetId = new BigInteger(request.getTargetId());
            List<NodeReference> kClosest = nodeLookup(targetId);
        }

        @Override
        public void findNode(Kademlia.FindNodeRequest request, StreamObserver<Kademlia.FindNodeResponse> responseObserver) {
            BigInteger targetId = new BigInteger(request.getTargetId());
            List<NodeReference> kClosest = routingTable.findKClosest(targetId);

            Kademlia.FindNodeResponse.Builder response = Kademlia.FindNodeResponse.newBuilder();
            kClosest.forEach(node -> {
                Kademlia.NodeReference.Builder nodeBuilder = Kademlia.NodeReference.newBuilder()
                        .setIp(node.getIp())
                        .setPort(node.getPort())
                        .setId(node.getId().toString());
                response.addKClosest(nodeBuilder);
            });

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void ping(Kademlia.Empty request, StreamObserver<Kademlia.Empty> responseObserver) {
            super.ping(request, responseObserver);
        }
    }






}
