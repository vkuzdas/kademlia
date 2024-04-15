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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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
     * Republish task handle for cancellation/rescheduling
     */
    private final Map<BigInteger, ScheduledFuture<?>> republishTasks = Collections.synchronizedMap(new HashMap<>());

    // TODO: Expiration goes against single node publish optimization?
    /**
     * Expiration task handle for cancellation/rescheduling
     */
    private final Map<BigInteger, ScheduledFuture<?>> expireTasks = Collections.synchronizedMap(new HashMap<>());

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);


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
     * Time after which the original publisher must republish a key/value pair <br>
     * Note: For opt-1, original protocol assumes a certain network delay which results in non-racing intervals between the nodes.
     * Since this implementation runs mainly locally, we need to 'desynchronize' the republishing intervals
     */
    private static Duration republishInterval = Duration.ofMinutes(15);

    /**
     * Time after which a key/value pair expires; this is a time-to-live (TTL) from the original publication date
     */
    private static Duration expireInterval = Duration.ofMinutes(15).plus(Duration.ofSeconds(10));

    /**
     * Whether to <b>forcefully</b> desynchronize republishing intervals. <br>
     * Kademlia paper presents republishing optimization in which only the first republishing node republishes key. This however requires network delay assumption. By setting desynchronization to true, Node simulates network delay under local conditions.
     */
    private static boolean desynchronizeRepublishInterval = false;
    private final int simulatedNetworkDelay = new Random().nextInt(800)+200;


    ///////////////////////////////
    ///  NODE-STATE INITIATION  ///
    ///////////////////////////////

    public KademliaNode(String ip, int port) {
        this.self = new NodeReference(ip, port);
        this.routingTable = new RoutingTable(ID_LENGTH, ALPHA_PARAMETER, K_PARAMETER, self);

        server = ServerBuilder.forPort(port)
                .addService(new KademliaNodeServer())
                .build();
    }

    /**
     * Whether to desynchronize republishing intervals
     */
    public static void setDesynchronizeRepublishInterval(boolean desync) {
        desynchronizeRepublishInterval = desync;
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

    @VisibleForTesting
    public static void setRepublishInterval(Duration duration) {
        republishInterval = duration;
    }

    @VisibleForTesting
    public static void setExpireInterval(Duration duration) {
        expireInterval = duration;
    }


    @VisibleForTesting
    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    @VisibleForTesting
    public Map<BigInteger, String> getLocalData() {
        return localData;
    }


    ////////////////////////////////
    ///  CLIENT-SIDE PROCESSING  ///
    ////////////////////////////////

    ////  Node run administration  ////

    public void initKademlia() throws IOException {
        startServer();
    }

    private void startServer() throws IOException {
        server.start();
        logger.warn("[{}]  Server started, listening on {}", self, self.port);
    }

    public void leave() {
        logger.warn("[{}]  Leaves the network", self);
        shutdownKademliaNode();
    }

    public void shutdownKademliaNode() {
        logger.warn("[{}]  Initiated node shutdown!", self);
        stopServer();
        republishTasks.forEach((k, v) -> v.cancel(true));
        executor.shutdownNow();
    }

    public void stopServer() {
        if (server != null) {
            server.shutdownNow();
            logger.warn("[{}]  Server stopped, listening on {}", self, self.port);
        }
    }


    ////  RPC  calls to other nodes  ////

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
//            logger.trace("[{}]  JOIN - refresh: looking up {}", self, rangeStart);
            List<NodeReference> kBestInRange = nodeLookup(rangeStart, self.toProto());

            // nodeLookup with specified joiningNode returns all nodes found during the lookup
            kBestInRange.stream().limit(K_PARAMETER).forEach(routingTable::insert);
        }

        logger.warn("[{}]  Joined KadNetwork!", self);
    }

    /**
     * Locate K globally-closest nodes to the targetId <br>
     * If joiningNode is not null, all nodes found during the lookup are returned
     * @param joiningNode - node that is joining the network, null if it's a regular lookup
     * @return List of nodes that were found during the lookup <br>
     */
    private List<NodeReference> nodeLookup(BigInteger targetId, Kademlia.NodeReference joiningNode) {
        logger.trace("[{}]  initiating nodeLookup", self);

        if (routingTable.getSize() == 0) {
            logger.trace("[{}]  My routing table is empty", self);
            return new ArrayList<>();
        }

        List<NodeReference> k_best = routingTable.findKClosest(targetId);
        HashSet<NodeReference> allFoundNodes = new HashSet<>(k_best);
        Set<NodeReference> failedNodes = new HashSet<>();

        while (true) { // while better results are coming

            Set<NodeReference> foundInOneIteration = multicastFindNode(new ArrayList<>(k_best), targetId, joiningNode, failedNodes);
            if (!failedNodes.isEmpty()) {
                for (NodeReference failedNode : failedNodes) {
                    routingTable.remove(failedNode);
                    k_best.remove(failedNode);
                    foundInOneIteration.remove(failedNode);
                    allFoundNodes.remove(failedNode);
                }
            }

            if (joiningNode != null)
                allFoundNodes.addAll(foundInOneIteration);

            BigInteger currBest = getBestDistance(k_best, targetId);
            BigInteger newBest = getBestDistance(foundInOneIteration, targetId);

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
     * Makes at most ALPHA concurrent calls (depends on the size of toQuery)
     * Returns all nodes found during the lookup
     */
    private Set<NodeReference> multicastFindNode(List<NodeReference> toQuery, BigInteger targetId, Kademlia.NodeReference joiningNode, Set<NodeReference> failedNodes) {
        toQuery.remove(self); // do not query self
        Set<NodeReference> foundInOneIteration = new HashSet<>();

        while (!toQuery.isEmpty()) {
            int calls = Math.min(ALPHA_PARAMETER, toQuery.size());
            CountDownLatch latch = new CountDownLatch(calls);
            Set<NodeReference> found = Collections.synchronizedSet(new HashSet<>());

            for (int i = 0; i < calls; i++) {
                NodeReference recipient = toQuery.remove(toQuery.size() - 1);

                ManagedChannel channel = ManagedChannelBuilder.forTarget(recipient.getAddress()).usePlaintext().build();
                Kademlia.FindNodeRequest.Builder request = Kademlia.FindNodeRequest.newBuilder()
                        .setTargetId(targetId.toString())
                        .setSender(self.toProto());
                if (joiningNode != null)
                    request.setJoiningNode(joiningNode);
                KademliaServiceGrpc.newStub(channel).findNode(request.build(), new StreamObserver<Kademlia.FindNodeResponse>() {
                    @Override
                    public void onNext(Kademlia.FindNodeResponse findNodeResponse) {
                        findNodeResponse.getKClosestList().stream().map(NodeReference::new).forEach(found::add);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("[{}]  multicastFindNode: Error while finding node[{}]: {}", self, recipient, throwable.toString());
                        failedNodes.add(recipient);
                        latch.countDown();
                        channel.shutdown();
                    }

                    @Override
                    public void onCompleted() {
                        routingTable.insert(recipient);
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
            putAndSchedule(keyHash, value);
            return;
        }

        List<NodeReference> kClosest = nodeLookup(keyHash, null);
        CountDownLatch latch = new CountDownLatch(kClosest.size());

        for (NodeReference node : kClosest) {
            if (node.equals(self)) {
                putAndSchedule(keyHash, value);
                latch.countDown();
                continue;
            }

            ManagedChannel channel = ManagedChannelBuilder.forTarget(node.getAddress()).usePlaintext().build();
            Kademlia.StoreRequest request = Kademlia.StoreRequest.newBuilder()
                    .setKey(keyHash.toString())
                    .setValue(value)
                    .setSender(self.toProto())
                    .build();

            KademliaServiceGrpc.newStub(channel).store(request, new StreamObserver<Kademlia.StoreResponse>() {
                @Override
                public void onNext(Kademlia.StoreResponse storeResponse) {}
                @Override
                public void onError(Throwable throwable) {
                    logger.error("[{}]  STORE: Error while storing key[{}] on node[{}]: {}", self, keyHash, node, throwable.toString());
                    routingTable.remove(node);
                    latch.countDown();
                    channel.shutdown();
                }
                @Override
                public void onCompleted() {
                    routingTable.insert(node);
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
        CountDownLatch latch = new CountDownLatch(kClosest.size());

        for(NodeReference node : kClosest) {

            if (node.equals(self)) {
                result.add(new Pair(self, localData.get(keyHash)));
                latch.countDown();
                continue;
            }

            ManagedChannel channel = ManagedChannelBuilder.forTarget(node.getAddress()).usePlaintext().build();
            Kademlia.RetrieveRequest.Builder request = Kademlia.RetrieveRequest.newBuilder()
                    .setSender(self.toProto())
                    .setKey(keyHash.toString());

            KademliaServiceGrpc.newStub(channel).retrieve(request.build(), new StreamObserver<Kademlia.RetrieveResponse>() {
                @Override
                public void onNext(Kademlia.RetrieveResponse response) {
                    if (response.getStatus() == Kademlia.Status.SUCCESS) {
                        result.add(new Pair(node, response.getValue()));
                    } else if (response.getStatus() == Kademlia.Status.NOT_FOUND) {
                        result.add(new Pair(node, null));
                    }
                }

                @Override
                public void onError(Throwable t) {
                    routingTable.remove(node);
                    logger.error("[{}]  RETRIEVE: Error while finding node[{}]: {}", self, node, t.toString());
                    latch.countDown();
                    channel.shutdown();
                }

                @Override
                public void onCompleted() {
                    routingTable.insert(node);
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

        return result;
    }

    /**
     * Delete key-value pair from the K-closest nodes to the keyhash
     */
    public void delete(String key) {
        BigInteger keyHash = getId(key);

        if(routingTable.getSize() == 0) {
            localData.remove(keyHash);
            return;
        }

        List<NodeReference> kClosest = nodeLookup(keyHash, null);
        CountDownLatch latch = new CountDownLatch(kClosest.size());

        for (NodeReference node : kClosest) {

            if (node.equals(self)){
                deleteAndDeschedule(keyHash);
                latch.countDown();
                continue;
            }

            ManagedChannel channel = ManagedChannelBuilder.forTarget(node.getAddress()).usePlaintext().build();
            Kademlia.DeleteRequest.Builder request = Kademlia.DeleteRequest.newBuilder()
                    .setKey(keyHash.toString())
                    .setSender(self.toProto());
            KademliaServiceGrpc.newStub(channel).delete(request.build(), new StreamObserver<Kademlia.DeleteResponse>() {
                @Override
                public void onNext(Kademlia.DeleteResponse deleteResponse) {/*do nothing when response*/}

                @Override
                public void onError(Throwable throwable) {
                    routingTable.remove(node);
                    logger.error("[{}]  DELETE: Error while finding node[{}]: {}", self, node, throwable.toString());
                    latch.countDown();
                    channel.shutdown();
                }

                @Override
                public void onCompleted() {
                    routingTable.insert(node);
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
    }


    ////  Utility methods  ////

    private void putAndSchedule(BigInteger keyHash, String value) {
        localData.put(keyHash, value);

        ScheduledFuture<?> republishTimer = executor.scheduleAtFixedRate(getRepublishTask(keyHash, value), getRepublishDelay(), republishInterval.toMillis(), TimeUnit.MILLISECONDS);
        republishTasks.put(keyHash, republishTimer);

        Runnable expireTask = () -> {
            logger.trace("[{}]  Key[{}] expired!", self, keyHash);
            localData.remove(keyHash);
        };
        ScheduledFuture<?> expireTimer = executor.schedule(expireTask, expireInterval.toMillis(), TimeUnit.MILLISECONDS);
        expireTasks.put(keyHash, expireTimer);
    }

    private long getRepublishDelay() {
        // simulate network delay under local conditions to allow for single node republish
        return desynchronizeRepublishInterval ? simulatedNetworkDelay + republishInterval.toMillis() : republishInterval.toMillis();
    }

    private Runnable getRepublishTask(BigInteger keyHash, String value) {
        return () -> {
            List<NodeReference> kClosest = nodeLookup(keyHash, null);
            kClosest.remove(self);
            CountDownLatch latch = new CountDownLatch(kClosest.size());

            logger.debug("[{}]  Asynchronously republishing key {} to k-closest: {}", self, keyHash, kClosest);

            for (NodeReference node : kClosest) {
                ManagedChannel channel = ManagedChannelBuilder.forTarget(node.getAddress()).usePlaintext().build();
                Kademlia.StoreRequest request = Kademlia.StoreRequest.newBuilder()
                        .setKey(keyHash.toString())
                        .setValue(value)
                        .setSender(self.toProto())
                        .build();
                KademliaServiceGrpc.newStub(channel).store(request, new StreamObserver<Kademlia.StoreResponse>() {
                    @Override
                    public void onNext(Kademlia.StoreResponse storeResponse) {}
                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("[{}]  republish: Error while storing key[{}] on node[{}]: {}", self, keyHash, node, throwable.toString());
                        routingTable.remove(node);
                        latch.countDown();
                        channel.shutdown();
                    }
                    @Override
                    public void onCompleted() {
                        routingTable.insert(node);
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
        };
    }

    private BigInteger getBestDistance(Collection<NodeReference> collection, BigInteger targetId) {
        return collection.stream()
                .map(n -> targetId.subtract(n.getId()).abs())
                .min(Comparator.naturalOrder())
                .orElse(null);
    }

    private void deleteAndDeschedule(BigInteger keyhash) {
        localData.remove(keyhash);
        republishTasks.get(keyhash).cancel(true);
        expireTasks.get(keyhash).cancel(true);
    }


    ////////////////////////////////
    ///  SERVER-SIDE PROCESSING  ///
    ////////////////////////////////
    
    /**
     * Server-side of Kademlia node
     */
    private class KademliaNodeServer extends KademliaServiceGrpc.KademliaServiceImplBase {

        /**
         * Initiated on joining node, sent to Booststrap node
         */
        @Override
        public void promptNodeLookup(Kademlia.LookupRequest request, StreamObserver<Kademlia.LookupResponse> responseObserver) {
            NodeReference joiningNode = new NodeReference(request.getJoiningNode());
            logger.trace("[{}]  Node lookup initiated from [{}]", self, joiningNode);

            List<NodeReference> kClosest = nodeLookup(new BigInteger(request.getTargetId()), request.getJoiningNode());
            routingTable.insert(joiningNode); // break the "insert most recently contacted" rule to not query the joining node

            Kademlia.LookupResponse.Builder response = Kademlia.LookupResponse.newBuilder()
                    .addAllFoundNodes(kClosest.stream().map(NodeReference::toProto).collect(Collectors.toList()));

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        /**
         * Instructs a node to store a key-value pair for later retrieval
         */
        @Override
        public void store(Kademlia.StoreRequest request, StreamObserver<Kademlia.StoreResponse> responseObserver) {
            routingTable.insert(new NodeReference(request.getSender()));
            logger.trace("[{}]  Received STORE rpc from {}", self, request.getSender().getPort());

            BigInteger key = new BigInteger(request.getKey());
            String value = request.getValue();

            if (!localData.containsKey(key)) {
                putAndSchedule(key, value);
            }
            else {
                // opt-1: if key exists, postpone republish
                if (!localData.get(key).equals(value)) {
                    localData.put(key, value);
                }
                republishTasks.get(key).cancel(true);
                executor.scheduleAtFixedRate(getRepublishTask(key, value), getRepublishDelay(), republishInterval.toMillis(), TimeUnit.MILLISECONDS);
            }

            responseObserver.onNext(Kademlia.StoreResponse.newBuilder().setStatus(Kademlia.Status.SUCCESS).build());
            responseObserver.onCompleted();
        }

        /**
         * Instructs a node to retrieve the value associated with the given key
         */
        @Override
        public void retrieve(Kademlia.RetrieveRequest request, StreamObserver<Kademlia.RetrieveResponse> responseObserver) {
            routingTable.insert(new NodeReference(request.getSender()));

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
            routingTable.insert(new NodeReference(request.getSender()));
//            logger.trace("[{}]  Received FIND_NODE rpc", self);

            BigInteger targetId = new BigInteger(request.getTargetId());
            List<NodeReference> kClosest = routingTable.findKClosest(targetId);

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

//            logger.trace("[{}]  Sending FIND_NODE response", self);

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void delete(Kademlia.DeleteRequest request, StreamObserver<Kademlia.DeleteResponse> responseObserver) {
            routingTable.insert(new NodeReference(request.getSender()));

            BigInteger key = new BigInteger(request.getKey());
            String value = localData.get(key);

            Kademlia.DeleteResponse response;
            if (value == null) {
                response = Kademlia.DeleteResponse.newBuilder()
                        .setStatus(Kademlia.Status.NOT_FOUND)
                        .build();
            } else {
                deleteAndDeschedule(key);
                response = Kademlia.DeleteResponse.newBuilder()
                        .setStatus(Kademlia.Status.SUCCESS)
                        .build();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void ping(Kademlia.Empty request, StreamObserver<Kademlia.Empty> responseObserver) {
            super.ping(request, responseObserver);
        }
    }

}
