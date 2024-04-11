package kademlia;

import com.google.common.annotations.VisibleForTesting;
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

import static kademlia.Util.*;

public class KademliaNode {

    private static final Logger logger = LoggerFactory.getLogger(KademliaNode.class);

    private final NodeReference self;

    /**
     * Local data storage
     */
    private final Map<BigInteger, String> localData = Collections.synchronizedMap(new HashMap<>());


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


    /**
     * There is high probability that new Kad node is inserted in the last KB
     * which means there would be no refresh. <br>
     * We need to set custom id for testing purposes.
     */
    @VisibleForTesting
    public KademliaNode(String ip, int port, BigInteger id) {
        this.self = new NodeReference(ip, port, id);
        this.routingTable = new RoutingTable(ID_LENGTH, ALPHA_PARAMETER, K_PARAMETER, self);

        server = ServerBuilder.forPort(port)
                .addService(new KademliaNodeServer())
                .build();
    }

    public KademliaNode(String ip, int port) {
        this.self = new NodeReference(ip, port);
        this.routingTable = new RoutingTable(ID_LENGTH, ALPHA_PARAMETER, K_PARAMETER, self);

        server = ServerBuilder.forPort(port)
                .addService(new KademliaNodeServer())
                .build();
    }

    @VisibleForTesting
    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    @VisibleForTesting
    public Map<BigInteger, String> getLocalData() {
        return localData;
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
     * When new node J is joining, it contacts bootsrap node B <br>
     * J inserts B into appropriate K-bucket <br>
     * J then prompts W to lookup U.id <br>
     * Finally, J will refresh all K-buckets further away than the B's K-bucket
     */
    public void join(NodeReference bootstrap) throws IOException {
        startServer();

        logger.warn("[{}]  Joining KadNetwork!", self);

        routingTable.insert(bootstrap);

        // prompt bootstrap to do lookup for an ID
        ManagedChannel channel = ManagedChannelBuilder.forTarget(bootstrap.getAddress()).usePlaintext().build();
        Kademlia.LookupRequest request = Kademlia.LookupRequest.newBuilder()
                .setTargetId(self.getId().toString())
                .setJoiningNode(self.toProto())
                .build();

        logger.debug("[{}]  JOIN - prompting boostrap node [{}] for myId lookup", self, bootstrap);
        Kademlia.LookupResponse response = KademliaServiceGrpc.newBlockingStub(channel).promptNodeLookup(request);
        channel.shutdown();

        response.getFoundNodesList().forEach(n -> routingTable.insert(new NodeReference(n)));

        // refresh all KB further away than the B's KB (refresh = lookup for random id in bucket range)
        // Note: some sources suggest to refresh all KB
        int bootstrapIndex = routingTable.getBucketIndex(bootstrap.getId());
        logger.debug("[{}]  JOIN - initiating refresh from {}th KB", self, bootstrapIndex);
        for (int i = bootstrapIndex+1; i < ID_LENGTH; i++) {
            BigInteger rangeStart = BigInteger.valueOf(2).pow(i);
            logger.trace("[{}]  JOIN - refresh: looking up {}", self, rangeStart);
            List<NodeReference> kBestInRange = nodeLookup(rangeStart, self.toProto());

            // nodeLookup with specified joiningNode returns all nodes found during the lookup
            kBestInRange.stream().limit(K_PARAMETER).forEach(routingTable::insert);
        }

        logger.warn("[{}]  Joined KadNetwork!", self);
    }

    private void getKeys(List<NodeReference> myClosest) {
        // TODO: can be done in parallel
        myClosest.forEach(node -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(node.getAddress()).usePlaintext().build();
            Kademlia.MoveKeysRequest request = Kademlia.MoveKeysRequest.newBuilder()
                    .setJoiningNodeId(self.getId().toString())
                    .setSenderIp(self.getIp())
                    .setSenderPort(self.getPort())
                    .build();
            Kademlia.MoveKeysResponse response = KademliaServiceGrpc.newBlockingStub(channel).moveKeys(request);
            channel.shutdown();
            response.getEntriesList().forEach(e -> localData.put(new BigInteger(e.getKey()), e.getValue()));
        });
    }

    /**
     * Locate K globally-closest nodes to the targetId <br>
     * If joiningNode is not null, all nodes found during the lookup are returned
     * @param joiningNode - node that is joining the network, null if it's a regular lookup
     * @return List of nodes that were found during the lookup <br>
     */
    // TODO: think about joining node case separation
    private List<NodeReference> nodeLookup(BigInteger targetId, Kademlia.NodeReference joiningNode) {
        logger.trace("[{}]  initiating nodeLookup", self);

        if (routingTable.getSize() == 0) {
            logger.trace("[{}]  My routing table is empty", self);
            return new ArrayList<>();
        }

        List<NodeReference> k_best = routingTable.findKClosest(targetId);
        HashSet<NodeReference> allFoundNodes = new HashSet<>(k_best);

        while (true) { // while better results are coming

            Set<NodeReference> foundInOneIteration = multicastFindNode(new ArrayList<>(k_best), targetId, joiningNode);

            if (joiningNode != null) {
                allFoundNodes.addAll(foundInOneIteration);
            }

            BigInteger currBest = getBestDistance(k_best, targetId);
            BigInteger newBest = getBestDistance(foundInOneIteration, targetId);



            // update k_best: remove duplicates, sort by distance to target, keep only K best
            foundInOneIteration.addAll(k_best);

            k_best = foundInOneIteration.stream()
                    .sorted(Comparator.comparing(node -> targetId.subtract(node.getId()).abs()))
                    .limit(K_PARAMETER)
                    .collect(Collectors.toList());

            // Terminate when same or worse node found
            if (currBest == null || newBest == null || newBest.compareTo(currBest) >= 0) {
                break;
            }
        }

        if (joiningNode != null) {
            logger.trace("[{}]  Node lookup finished (KB was not full)", self);
            // all nodes found during the lookup = all nodes into which joiningNode was inserted to
            return new ArrayList<>(allFoundNodes);
        }

        logger.trace("[{}]  Node lookup finished (whole KB returned)", self);
        return k_best;
    }

    /**
     * Remove and return keys with XOR-distance lesser to targetId than to selfId <br>
     * Since each key is placed according to XOR distance, whole map has to be iterated over to see which keys are closer to the lastly joined node
     */
    private List<Map.Entry<BigInteger, String>> extractKeys(BigInteger targetId) {
        List<Map.Entry<BigInteger, String>> extracted = new ArrayList<>();

        synchronized (localData) {
            localData.forEach((k, v) -> {
                BigInteger currDistance = k.xor(self.getId());
                BigInteger joiningNodeDistance = k.xor(targetId);

                // extract nodes XOR-closer to joining node
                if (joiningNodeDistance.compareTo(currDistance) <= 0) {
                    extracted.add(new AbstractMap.SimpleEntry<>(k, v));
                }

            });
            extracted.forEach(e -> localData.remove(e.getKey()));
        }

        return extracted;
    }

    private BigInteger getBestDistance(Collection<NodeReference> collection, BigInteger targetId) {
        return collection.stream()
                .map(n -> targetId.subtract(n.getId()).abs())
                .min(Comparator.naturalOrder())
                .orElse(null);
    }

    /**
     * Makes at most ALPHA concurrent calls (depends on the size of toQuery)
     * Returns all nodes found during the lookup
     */
    private Set<NodeReference> multicastFindNode(List<NodeReference> toQuery, BigInteger targetId, Kademlia.NodeReference joiningNode) {
        toQuery.remove(self); // do not query self
        Set<NodeReference> foundInOneIteration = new HashSet<>();

        while (!toQuery.isEmpty()) {
            int calls = Math.min(ALPHA_PARAMETER, toQuery.size());
            CountDownLatch latch = new CountDownLatch(calls);
            Set<NodeReference> found = Collections.synchronizedSet(new HashSet<>());

            for (int i = 0; i < calls; i++) {
                NodeReference curr = toQuery.remove(toQuery.size() - 1);

                ManagedChannel channel = ManagedChannelBuilder.forTarget(curr.getAddress()).usePlaintext().build();
                Kademlia.FindNodeRequest.Builder request = Kademlia.FindNodeRequest.newBuilder()
                        .setTargetId(targetId.toString())
                        .setSender(self.toProto());
                if (joiningNode != null)
                    request.setJoiningNode(joiningNode);
                KademliaServiceGrpc.newStub(channel).findNode(request.build(), new StreamObserver<Kademlia.FindNodeResponse>() {
                    // TODO: sometimes throws SEVERE: *~*~*~ Channel ManagedChannelImpl{logId=8948, target=localhost:10023} was not terminated properly!!! ~*~*~*
                    //    Make sure to call shutdown()/shutdownNow() and wait until awaitTermination() returns true.
                    @Override
                    public void onNext(Kademlia.FindNodeResponse findNodeResponse) {
                        findNodeResponse.getKClosestList().stream().map(NodeReference::new).forEach(found::add);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        // TODO: remove node ?
                        logger.error("[{}]  Error while finding node: {}", self, throwable.toString());
                        latch.countDown();
                        channel.shutdown();
                    }

                    @Override
                    public void onCompleted() {
                        // since the response is unary, this function may be redundant
                        latch.countDown();
                        channel.shutdown();
                    }
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("[{}]  Waiting for async calls interrupted", self, e);
            }

            foundInOneIteration.addAll(found);
        }
        return foundInOneIteration;
    }

    /**
     * Store key-value pair on the K-closest nodes to the keyhash
     */
    public void put(String key, String value) {
        BigInteger keyHash = getId(key);

        if(routingTable.getSize() == 0) {
            localData.put(keyHash, value);
            return;
        }

        List<NodeReference> kClosest = nodeLookup(keyHash, null);

        kClosest.forEach(node -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(node.getAddress()).usePlaintext().build();
            Kademlia.StoreRequest request = Kademlia.StoreRequest.newBuilder()
                    .setKey(keyHash.toString())
                    .setValue(value)
                    .setSender(self.toProto())
                    .build();
            Kademlia.StoreResponse response = KademliaServiceGrpc.newBlockingStub(channel).store(request);
            channel.shutdown();
        });
    }

    /**
     * Retrieve value associated with the key from the K-closest nodes to the keyhash
     */
    public List<Pair> get(String key) {
        BigInteger keyHash = getId(key);

        if(routingTable.getSize() == 0) {
            return Collections.singletonList(new Pair(self, localData.get(keyHash)));
        }

        List<NodeReference> kClosest = nodeLookup(keyHash, null);
        List<Pair> result = new ArrayList<>();

        // TODO: can be done in parallel
        kClosest.forEach(node -> {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(node.getAddress()).usePlaintext().build();
            Kademlia.RetrieveRequest.Builder request = Kademlia.RetrieveRequest.newBuilder()
                    .setKey(keyHash.toString());
            Kademlia.RetrieveResponse response = KademliaServiceGrpc.newBlockingStub(channel).retrieve(request.build());
            channel.shutdown();

            result.add(new Pair(node, response.getValue()));
        });

        return result;
    }



    ////////////////////////////////
    ///  SERVER-SIDE PROCESSING  ///
    ////////////////////////////////
    
    /**
     * Server-side of Kademlia node
     */
    private class KademliaNodeServer extends KademliaServiceGrpc.KademliaServiceImplBase {

        // TODO: When a Kademlia node receives any message (request or reply) from another node, it updates the
        //  appropriate k-bucket for the sender’s node ID


        /**
         * Initiated on joining node, sent to Booststrap node
         */
        @Override
        public void promptNodeLookup(Kademlia.LookupRequest request, StreamObserver<Kademlia.LookupResponse> responseObserver) {
            NodeReference joiningNode = new NodeReference(request.getJoiningNode());
            logger.trace("[{}]  Node lookup initiated from [{}]", self, joiningNode);

            List<NodeReference> kClosest = nodeLookup(new BigInteger(request.getTargetId()), request.getJoiningNode());

            routingTable.insert(joiningNode);

            Kademlia.LookupResponse.Builder response = Kademlia.LookupResponse.newBuilder()
                    .addAllFoundNodes(kClosest.stream().map(NodeReference::toProto).collect(Collectors.toList()));

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void moveKeys(Kademlia.MoveKeysRequest request, StreamObserver<Kademlia.MoveKeysResponse> responseObserver) {
            Kademlia.MoveKeysResponse.Builder response = Kademlia.MoveKeysResponse.newBuilder();
            extractKeys(new BigInteger(request.getJoiningNodeId()))
                    .forEach(entry -> {
                        response.addEntries(Kademlia.Entry.newBuilder()
                                .setKey(entry.getKey().toString())
                                .setValue(entry.getValue())
                                .build());
            });
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        /**
         * Instructs a node to store a key-value pair for later retrieval
         */
        @Override
        public void store(Kademlia.StoreRequest request, StreamObserver<Kademlia.StoreResponse> responseObserver) {
            BigInteger key = new BigInteger(request.getKey());
            String value = request.getValue();
            localData.put(key, value);

            responseObserver.onNext(Kademlia.StoreResponse.newBuilder().setStatus(Kademlia.Status.SUCCESS).build());
            responseObserver.onCompleted();
        }

        /**
         * Instructs a node to retrieve the value associated with the given key
         */
        @Override
        public void retrieve(Kademlia.RetrieveRequest request, StreamObserver<Kademlia.RetrieveResponse> responseObserver) {
            BigInteger key = new BigInteger(request.getKey());
            String value = localData.get(key);

            Kademlia.RetrieveResponse response;
            if (value == null) {
                response = Kademlia.RetrieveResponse.newBuilder()
                        .setStatus(Kademlia.Status.NOT_FOUND)
                        .build();
            } else {
                response = Kademlia.RetrieveResponse.newBuilder()
                        .setStatus(Kademlia.Status.SUCCESS)
                        .setValue(value)
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * Recipient returns k nodes it knows about closest to the target ID
         */
        @Override
        public void findNode(Kademlia.FindNodeRequest request, StreamObserver<Kademlia.FindNodeResponse> responseObserver) {
            logger.trace("[{}]  Received FIND_NODE rpc", self);

            BigInteger targetId = new BigInteger(request.getTargetId());
            List<NodeReference> kClosest = routingTable.findKClosest(targetId);

            routingTable.insert(new NodeReference(request.getSender()));

            if (request.hasJoiningNode()) {
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

            logger.trace("[{}]  Sending FIND_NODE response", self);

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void ping(Kademlia.Empty request, StreamObserver<Kademlia.Empty> responseObserver) {
            super.ping(request, responseObserver);
        }
    }

}
