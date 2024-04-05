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

    public NodeReference getNodeReference() {
        return self;
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

        logger.warn("[{}]  Joining KadNetwork!", self);

        // U insertne W do spravneho KB
        routingTable.insert(bootstrap);

        // prompt W to do lookup for an ID
        ManagedChannel ch = ManagedChannelBuilder.forTarget(bootstrap.getAddress()).usePlaintext().build();
        blockingStub = KademliaServiceGrpc.newBlockingStub(ch);
        Kademlia.LookupRequest request = Kademlia.LookupRequest.newBuilder()
                .setTargetId(self.getId().toString())
                .setJoiningNode(self.toProto())
                .setType(JOIN)
                .build();

        logger.trace("[{}]  Join - prompting node lookup", self);
        Kademlia.LookupResponse response = blockingStub.promptNodeLookup(request);
        response.getKClosestList().forEach(n -> routingTable.insert(new NodeReference(n)));

        // refresh all KB further away than the bootstrap's KB (refresh = lookup for random id in bucket range)
        // Note: some sources suggest to refresh all KB
        int bootstrapIndex = routingTable.getBucketIndex(bootstrap.getId());
        logger.trace("[{}]  Join - initiating refresh", self);
        for (int i = bootstrapIndex+1; i < ID_LENGTH; i++) {
            BigInteger rangeStart = BigInteger.valueOf(2).pow(i);
            List<NodeReference> kBestInRange = nodeLookup(rangeStart, self.toProto());

            // nodeLookup with specified joiningNode returns all nodes found during the lookup
            kBestInRange.stream().limit(K_PARAMETER).forEach(routingTable::insert);
        }

        logger.warn("[{}]  Joined KadNetwork!", self);
    }

    /**
     * Locate K globally-closest nodes to the targetId
     *  Note: if joiningNode is present, it returns all encountered nodes to initialize join's routing table
     *      as well as insert join node into other k-buckets
     */
    private List<NodeReference> nodeLookup(BigInteger targetId, Kademlia.NodeReference joiningNode) {

        if (routingTable.getSize() == 0) {
            logger.trace("[{}]  My routing table is empty", self);
            return new ArrayList<>();
        }

        List<NodeReference> k_best = routingTable.findKClosest(targetId);
        HashSet<NodeReference> allFoundNodes = new HashSet<>(k_best);

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
                    Kademlia.FindNodeRequest request = Kademlia.FindNodeRequest.newBuilder()
                            .setTargetId(targetId.toString())
                            .setJoiningNode(joiningNode)
                            .build();
                    KademliaServiceGrpc.newStub(channel).findNode(request, new StreamObserver<Kademlia.FindNodeResponse>() {
                        @Override
                        public void onNext(Kademlia.FindNodeResponse findNodeResponse) {
                            findNodeResponse.getKClosestList().stream().map(NodeReference::new).forEach(found::add);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            // TODO: remove ?
                            logger.error("[{}]  Error while finding node", self, throwable);
                        }

                        @Override
                        public void onCompleted() {
                            // since the response is unary, this function may be redundant
                            logger.debug("[{}]  Node lookup completed", self);
                        }
                    });
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.error("[{}]  Waiting for async calls interrupted", self, e);
                }
            }
            // done concurrently asking all K best nodes

            if (joiningNode != null) {
                allFoundNodes.addAll(found);
            }

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
                logger.trace("[{}]  Lookup terminated", self);
                break;
            }

            logger.trace("[{}]  Lookup continues ({} is newBestDist)", self, newBestDist);

            // update k_best: remove duplicates, sort by distance to target, keep only K best
            found.addAll(k_best);

            k_best = found.stream()
                    .distinct()
                    .sorted(Comparator.comparing(node -> targetId.subtract(node.getId()).abs()))
                    .limit(K_PARAMETER)
                    .collect(Collectors.toList());
        }

        if (joiningNode != null) {
            // all nodes found during the lookup, all nodes into which joiningNode was inserted to
            return new ArrayList<>(allFoundNodes);
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
            logger.trace("[{}]  Node lookup initiated from [{}]", self, request.getJoiningNode());

            List<NodeReference> kClosest = nodeLookup(new BigInteger(request.getTargetId()), request.getJoiningNode());

            routingTable.insert(new NodeReference(request.getJoiningNode()));

            Kademlia.LookupResponse.Builder response = Kademlia.LookupResponse.newBuilder()
                    .addAllKClosest(kClosest.stream().map(NodeReference::toProto).collect(Collectors.toList()));

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void findNode(Kademlia.FindNodeRequest request, StreamObserver<Kademlia.FindNodeResponse> responseObserver) {

            BigInteger targetId = new BigInteger(request.getTargetId());
            List<NodeReference> kClosest = routingTable.findKClosest(targetId);

            routingTable.insert(new NodeReference(request.getSender()));

            if (request.getJoiningNode() != null) {
                routingTable.insert(new NodeReference(request.getJoiningNode()));
            }

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
